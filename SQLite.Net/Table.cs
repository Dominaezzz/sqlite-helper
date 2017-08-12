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
	/// <summary>
	/// Class representing a Table in an SQLite Database.
	/// </summary>
	/// <typeparam name="T">A model class to represent the table.</typeparam>
    public class Table<T> : Query<T>
    {
	    private readonly SQLiteDatabase _db;

	    private readonly PropertyInfo[] _primaryKeys;
	    private readonly PropertyInfo[] _otherColumns;
	    private readonly PropertyInfo[] _insertColumns;
	    private readonly bool _hasIntegerPrimaryKey;

	    private readonly string _insertSQL;
	    private readonly string _insertWithConflictSQL;
		private readonly string _updateSQL;
	    private readonly string _updateWithConflictSQL;

		/// <summary>
		/// The name of the database table represented by this object.
		/// </summary>
		public string Name { get; }

	    internal Table(SQLiteQueryProvider provider, string name) : base(provider)
	    {
		    _db = provider.Database;
		    Name = name;

		    _primaryKeys = typeof(T).GetRuntimeProperties()
			    .Where(p => !p.IsDefined(typeof(IgnoreAttribute)) && p.IsDefined(typeof(PrimaryKeyAttribute)))
			    .ToArray();
		    _otherColumns = typeof(T).GetRuntimeProperties()
				.Where(p => !p.IsDefined(typeof(IgnoreAttribute)) && !p.IsDefined(typeof(PrimaryKeyAttribute)))
				.ToArray();

			if (_primaryKeys.Length == 1)
		    {
			    var property = _primaryKeys.Single();
			    var primaryKey = property.GetCustomAttribute<PrimaryKeyAttribute>();
			    if (primaryKey.AutoIncrement)
			    {
				    _insertColumns = _otherColumns;
				    _hasIntegerPrimaryKey = false;
			    }
			    else
				{
					_insertColumns = _primaryKeys.Concat(_otherColumns).ToArray();
					_hasIntegerPrimaryKey = Orm.IntegralTypes.Contains(property.PropertyType);
				}
		    }
			else
			{
				_insertColumns = _primaryKeys.Concat(_otherColumns).ToArray();
				_hasIntegerPrimaryKey = false;
			}

		    if (_primaryKeys.Length + _otherColumns.Length > 0)
		    {
				StringBuilder sb = new StringBuilder();
			    sb.Append("INSERT INTO [").Append(Name).Append("]");
			    sb.Append("(");
			    foreach (var property in _insertColumns)
			    {
				    sb.Append("[").Append(Orm.GetColumnName(property)).Append("]");
				    sb.Append(", ");
			    }
			    sb.Remove(sb.Length - 2, 2).Append(")");

			    sb.Append(" VALUES(")
				    .Append(string.Join(", ", Enumerable.Repeat("?", _insertColumns.Length)))
				    .Append(");");

			    _insertSQL = sb.ToString();
			    sb.Insert("INSERT".Length, " OR {0}");
			    _insertWithConflictSQL = sb.ToString();
			}

		    if (_primaryKeys.Length > 0 && _otherColumns.Length > 0)
		    {
				StringBuilder sb = new StringBuilder();

			    sb.Append("UPDATE [").Append(Name).Append("] SET ");
			    for (int i = 0; i < _otherColumns.Length; i++)
			    {
				    if (i > 0) sb.Append(", ");
				    sb.Append("[").Append(Orm.GetColumnName(_otherColumns[i])).Append("] = ?");
			    }

			    sb.Append(" WHERE ");
			    for (int i = 0; i < _primaryKeys.Length; i++)
			    {
				    if (i > 0) sb.Append(" && ");
				    sb.Append("[").Append(Orm.GetColumnName(_primaryKeys[i])).Append("] == ?");
			    }
			    sb.Append(";");

			    _updateSQL = sb.ToString();
			    sb.Insert("UPDATE".Length, " OR {0}");
			    _updateWithConflictSQL = sb.ToString();
		    }
	    }

	    /// <summary>
	    /// Inserts the given item into this table.
	    /// </summary>
	    /// <param name="item">The object representing a row to insert into the table.</param>
	    /// <param name="conflict">How to handle conflict.</param>
	    /// <returns>The number of succesfully insertes rows.</returns>
		public int Insert(T item, Conflict? conflict = null)
	    {
		    return Insert(Enumerable.Repeat(item, 1), conflict, false);
	    }

	    /// <summary>
	    /// Inserts the given items into this table.
	    /// </summary>
	    /// <param name="items">The objects representing rows to insert into the table.</param>
	    /// <param name="conflict">How to handle conflict.</param>
	    /// <param name="withTransaction">If true, will wrap the entire insert in a transaction.</param>
	    /// <param name="throwOnConflict">If true, will throw exception if a conflict occurs.</param>
	    /// <returns>The number of succesfully insertes rows.</returns>
		public int Insert(IEnumerable<T> items, Conflict? conflict = null, bool withTransaction = true, bool throwOnConflict = true)
		{
			string sql = conflict == null ? _insertSQL : string.Format(_insertWithConflictSQL, conflict.ToString().ToUpper());
			using (var statement = _db.CreateStatement(sql))
			{
				string savePoint = withTransaction ? _db.SavePoint() : null;
				int count = 0;
				foreach (var item in items)
				{
					int i = 0;
					if (_hasIntegerPrimaryKey)
					{
						var value = _insertColumns[i].GetValue(item);
						if (Convert.ToInt64(value) == default(long))
						{
							statement.BindNull(++i);
						}
						else
						{
							statement.Bind(++i, value);
						}
					}
					while (i < _insertColumns.Length)
					{
						var value = _insertColumns[i].GetValue(item);
						statement.Bind(++i, value);
					}
					try
					{
						statement.Execute();
						statement.Reset();
						count += _db.Changes;
					}
					catch (Exception)
					{
						if (withTransaction)
						{
							switch (conflict)
							{
								case Conflict.Rollback:
									_db.RollbackTo(savePoint); // Shouldn't need to call this since SQLite does it.
									count = 0;
									break;
								case null:
								case Conflict.Abort:
									_db.RollbackTo(savePoint);
									count = 0;
									break;
								case Conflict.Fail:
									_db.ReleaseSavePoint(savePoint);
									break;
								default:
									throw new ArgumentOutOfRangeException(nameof(conflict), "Should not happen for chosen conflict.");
							}
						}
						else
						{
							if (conflict == Conflict.Rollback)
							{
								count = 0;
							}
						}
						if (throwOnConflict) throw;
						return count;
					}
				}
				if (withTransaction) _db.ReleaseSavePoint(savePoint);
				return count;
			}
		}

	    /// <summary>
	    /// Updates the columns mapped to all the properties of the object except the primary keys.
	    /// Cannot be used to update primary key columns.
	    /// </summary>
	    /// <param name="item">The objects representing rows to update the table with.</param>
	    /// <param name="conflict">How to handle conflict.</param>
	    /// <returns>The number of succesful updates.</returns>
		public int Update(T item, Conflict? conflict = null)
	    {
		    return Update(Enumerable.Repeat(item, 1), conflict, false);
	    }

		/// <summary>
		/// Updates the columns mapped to all the properties of the objects except the primary keys.
		/// Cannot be used to update primary key columns.
		/// </summary>
		/// <param name="items">The objects representing rows to update the table with.</param>
		/// <param name="conflict">How to handle conflict.</param>
		/// <param name="withTransaction">If true, will wrap the entire update in a transaction.</param>
		/// <param name="throwOnConflict">If true, will throw exception if a conflict occurs.</param>
		/// <returns>The number of succesful updates.</returns>
		public int Update(IEnumerable<T> items, Conflict? conflict = null, bool withTransaction = true, bool throwOnConflict = true)
	    {
			if (_primaryKeys.Length == 0)
			{
				throw new InvalidOperationException("Must have at least one property declared as primary key.");
			}

		    string sql = conflict == null ? _updateSQL : string.Format(_updateWithConflictSQL, conflict.ToString().ToUpper());		    
		    using (var statement = _db.CreateStatement(sql))
		    {
				string savePoint = withTransaction ? _db.SavePoint() : null;
			    int count = 0;
			    foreach (var item in items)
			    {
				    int bindIndex = 1;
				    foreach (var property in _otherColumns.Concat(_primaryKeys))
				    {
					    var val = property.GetValue(item);
						statement.Bind(bindIndex, val);
					    bindIndex++;
				    }

				    try
				    {
					    statement.Execute();
					    statement.Reset();
					    count += _db.Changes;
					}
				    catch (Exception)
				    {
					    if (withTransaction)
					    {
						    switch (conflict)
						    {
							    case Conflict.Rollback:
								    _db.RollbackTo(savePoint); // Shouldn't need to call this since SQLite does it.
								    count = 0;
								    break;
							    case null:
							    case Conflict.Abort:
								    _db.RollbackTo(savePoint);
								    count = 0;
								    break;
							    case Conflict.Fail:
								    _db.ReleaseSavePoint(savePoint);
								    break;
							    default:
								    throw new ArgumentOutOfRangeException(nameof(conflict), "Should not happen for chosen conflict.");
						    }
					    }
					    else
					    {
						    if (conflict == Conflict.Rollback)
						    {
							    count = 0;
						    }
					    }
					    if (throwOnConflict) throw;
					    return count;
					}
			    }
			    if (withTransaction) _db.ReleaseSavePoint(savePoint);
			    return count;
		    }
	    }

		/// <summary>
		/// Deletes EVERY row in the table.
		/// </summary>
		/// <returns>The number of rows deleted.</returns>
		public int DeleteAll()
	    {
		    _db.Execute($"DELETE FROM [{Name}];");
		    return _db.Changes;
	    }

	    /// <summary>
	    /// Deletes every row in the table that satisfies the given predicate.
	    /// </summary>
	    /// <returns>The number of rows deleted.</returns>
		public int Delete(Expression<Func<T, bool>> predicate)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			
			var pred = PropertyReplacer.Replace(predicate.Body, predicate.Parameters[0]);
			pred = ((SQLiteQueryProvider) Provider).Translate(pred);

			if (pred is ProjectionExpression projection)
			{
				pred = projection.Source;
			}
			var args = new List<object>();
			var cmdText = $"DELETE FROM [{Name}] WHERE {QueryFormatter.Format(pred, args)};";
			
			_db.Execute(cmdText, args.ToArray());

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
