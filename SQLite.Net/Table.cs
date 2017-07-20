using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;
using SQLite.Net.Translation;

namespace SQLite.Net
{
    public class Table<T> : Query<T>
    {
	    private readonly SQLiteDatabase _db;

		public string Name { get; }

	    internal Table(SQLiteQueryProvider provider, string name) : base(provider)
	    {
		    _db = provider.Database;
		    Name = name;
	    }

	    public int Insert(T item, Conflict? conflict = null)
	    {
		    return Insert(Enumerable.Repeat(item, 1), conflict, false);
	    }

	    public int Insert(IEnumerable<T> items, Conflict? conflict = null, bool withTransaction = true)
	    {
			List<PropertyInfo> insertColumns = new List<PropertyInfo>();
		    StringBuilder sb = new StringBuilder();
		    sb.Append("INSERT");
		    if (conflict != null) sb.Append(" OR ").Append(conflict.ToString().ToUpper());

		    sb.Append(" INTO [").Append(Name).Append("]");
		    sb.Append("(");

		    int? integerPrimaryKeyIndex = null;
		    foreach (var property in typeof(T).GetRuntimeProperties())
		    {
			    if (property.IsDefined(typeof(IgnoreAttribute))) continue;

			    var primaryKey = property.GetCustomAttribute<PrimaryKeyAttribute>();
			    if (primaryKey != null)
			    {
				    if (primaryKey.AutoIncrement) continue;
				    if (Orm.IntegralTypes.Contains(property.PropertyType))
				    {
						integerPrimaryKeyIndex = insertColumns.Count;
				    }
			    }

			    string name = Orm.GetColumnName(property);

			    sb.Append("[").Append(name).Append("]");
			    sb.Append(", ");
				insertColumns.Add(property);
		    }
		    sb.Remove(sb.Length - 2, 2).Append(")");

		    sb.Append(" VALUES(")
				.Append(string.Join(", ", Enumerable.Repeat("?", insertColumns.Count)))
				.Append(");");

		    string savePoint = withTransaction ? _db.SavePoint() : null;
		    using (var statement = _db.CreateStatement(sb.ToString()))
			{
				int count = 0;
				foreach (var item in items)
				{
					for (int i = 0; i < insertColumns.Count; i++)
					{
						var val = insertColumns[i].GetValue(item);
						if (i == integerPrimaryKeyIndex && Convert.ToInt64(val) == default(long))
						{
							statement.BindNull(i + 1);
						}
						else
						{
							statement.Bind(i + 1, val);
						}
					}
					try
					{
						statement.Execute();
					}
					catch (Exception e)
					{
						if (withTransaction && conflict == Conflict.Rollback)
						{
							_db.RollbackTo(savePoint);
							return 0;
						}
						if (conflict == Conflict.Fail || conflict == Conflict.Abort)
						{
							break;
						}
						if (withTransaction) _db.RollbackTo(savePoint);
						throw;
					}
					statement.Reset();
					count += _db.Changes;
				}
				if (withTransaction) _db.ReleaseSavePoint(savePoint);
				return count;
			}
		}

	    public int Update(T item, Conflict? conflict = null)
	    {
		    return Update(Enumerable.Repeat(item, 1), conflict, false);
	    }

	    public int Update(IEnumerable<T> items, Conflict? conflict = null, bool withTransaction = true)
	    {
		    List<PropertyInfo> updateColumns = new List<PropertyInfo>();
		    StringBuilder sb = new StringBuilder();
		    sb.Append("UPDATE");
		    if (conflict != null) sb.Append(" OR ").Append(conflict.ToString().ToUpper());

		    sb.Append(" [").Append(Name).Append("] SET ");

		    PropertyInfo primaryKeyColumn = null;
		    foreach (var property in typeof(T).GetRuntimeProperties())
		    {
			    if (property.IsDefined(typeof(IgnoreAttribute))) continue;

			    var primaryKey = property.GetCustomAttribute<PrimaryKeyAttribute>();
			    if (primaryKey != null)
			    {
					if (primaryKeyColumn != null)
					{
						throw new InvalidOperationException("Table class must have only on property declared as primary key.");
					}
					primaryKeyColumn = property;
					continue;
			    }
			    sb.Append("[").Append(Orm.GetColumnName(property)).Append("] = ?");
			    sb.Append(", ");
			    updateColumns.Add(property);
		    }
		    sb.Remove(sb.Length - 2, 2);

		    if (primaryKeyColumn == null)
		    {
			    throw new InvalidOperationException("Table class must have a property declared as primary key.");
		    }

		    sb.Append(" WHERE [").Append(Orm.GetColumnName(primaryKeyColumn)).Append("] == ?;");

		    string savePoint = withTransaction ? _db.SavePoint() : null;
		    using (var statement = _db.CreateStatement(sb.ToString()))
		    {
			    int count = 0;
			    foreach (var item in items)
			    {
				    for (int i = 0; i < updateColumns.Count; i++)
				    {
					    var val = updateColumns[i].GetValue(item);
						statement.Bind(i + 1, val);
				    }
					statement.Bind(updateColumns.Count + 1, primaryKeyColumn.GetValue(item));

				    try
				    {
					    statement.Execute();
				    }
				    catch (Exception)
				    {
					    if (withTransaction && conflict == Conflict.Rollback)
					    {
						    _db.RollbackTo(savePoint);
						    return 0;
					    }
					    if (conflict == Conflict.Fail || conflict == Conflict.Abort)
					    {
						    break;
					    }
					    if (withTransaction) _db.RollbackTo(savePoint);
					    throw;
				    }
				    statement.Reset();
				    count += _db.Changes;
			    }
			    if (withTransaction) _db.ReleaseSavePoint(savePoint);
			    return count;
		    }
	    }

		public int DeleteAll()
	    {
		    _db.Execute($"DELETE FROM [{Name}];");
		    return _db.Changes;
	    }

		public int Delete(Expression<Func<T, bool>> predicate)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			
			var pred = PropertyReplacer.Replace(predicate.Body, predicate.Parameters[0]);
			pred = ((SQLiteQueryProvider) Provider).Translate(pred);

			if (pred is ProjectionExpression projection)
			{
				pred = projection.Source;
			}
//			var args = new List<object>();
			var cmdText = $"DELETE FROM [{Name}] WHERE {QueryFormatter.Format(pred)};";
			
			_db.Execute(cmdText);

			return _db.Changes;
		}

	    public override string ToString()
	    {
		    return Name;
	    }

		private class PropertyReplacer : DbExpressionVisitor
		{
			private readonly List<ParameterExpression> _parameters;

			private PropertyReplacer(List<ParameterExpression> parameters)
			{
				_parameters = parameters;
			}

			public static Expression Replace(Expression expression, params ParameterExpression[] parameters)
			{
				return new PropertyReplacer(parameters.ToList()).Visit(expression);
			}

			protected override Expression VisitMember(MemberExpression node)
			{
				if (_parameters.Contains(node.Expression) && node.Member is PropertyInfo property)
				{
					return new ColumnExpression(node.Type, "", Orm.GetColumnName(property));
				}
				return base.VisitMember(node);
			}
		}
    }
}
