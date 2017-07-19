using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;
using SQLite.Net.Builders;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;
using SQLite.Net.Translation;
using SQLitePCL;

namespace SQLite.Net
{
	public class SQLiteException : Exception
	{
		public SQLiteException() { }

		public SQLiteException(string message) : base(message) { }
	}

	public enum TransactionType
	{
		Deferred, Immediate, Exclusive
	}

	public abstract class SQLiteDatabase : IDisposable
    {
		static SQLiteDatabase() { Batteries_V2.Init(); }

	    private readonly sqlite3 _db;
	    private readonly SQLiteQueryProvider _provider;

		public string Path { get; }
		public bool IsOpen { get; private set; }
		public TextWriter Logger { get; set; }

	    private readonly Stack<string> _transactionStack = new Stack<string>();
	    public int TransactionDepth => _transactionStack.Count;
	    public bool IsInTransaction => TransactionDepth > 0;

	    public long LastInsertRowId => raw.sqlite3_last_insert_rowid(_db);
	    public int Changes => raw.sqlite3_changes(_db);
	    public int TotalChanges => raw.sqlite3_total_changes(_db);

	    public TimeSpan BusyTimeout
		{
		    get => TimeSpan.FromMilliseconds(ExecuteScalar<long>("PRAGMA busy_timeout;"));
			set => raw.sqlite3_busy_timeout(_db, (int) value.TotalMilliseconds);
	    }

	    public long UserVersion
	    {
		    get => ExecuteScalar<long>("PRAGMA user_version");
		    set => Execute($"PRAGMA user_version = {value}");
	    }

	    public bool ForeignKeysEnabled
	    {
			get => ExecuteScalar<bool>("PRAGMA foreign_keys");
		    set => Execute($"PRAGMA foreign_keys = {value}");
		}

		public Table<DatabaseObject> SQLiteMaster { get; set; }
	    public Table<DatabaseObject> SQLiteTempMaster { get; set; }


		protected SQLiteDatabase(string path = null)
	    {
		    if (raw.sqlite3_open(path ?? ":memory:", out _db) != raw.SQLITE_OK)
		    {
			    throw new SQLiteException("Could not open database");
		    }
		    Path = path;
		    IsOpen = true;

			_provider = new SQLiteQueryProvider(this);

			SQLiteMaster = new Table<DatabaseObject>(_provider, "SQLITE_MASTER");
		    SQLiteTempMaster = new Table<DatabaseObject>(_provider, "SQLITE_TEMP_MASTER");

			foreach (var property in GetType().GetRuntimeProperties())
		    {
			    if (property.GetValue(this) != null) continue;

			    Type tableType = property.PropertyType;
		        TypeInfo typeInfo = tableType.GetTypeInfo();

			    if (!typeInfo.IsGenericType) continue;
		        Type genericeType = tableType.GetGenericTypeDefinition();
                if(!(typeof(Table<>) == genericeType || typeof(View<>) == genericeType)) continue;

			    Type type = typeInfo.GenericTypeArguments[0];
                
			    string name = type.GetTypeInfo().GetCustomAttribute<TableAttribute>()?.Name ?? property.Name;
				
			    var constructor = typeInfo.DeclaredConstructors.First();
				property.SetValue(this, constructor.Invoke(new object[]{ _provider, name }));
		    }
	    }

		public void Dispose()
	    {
		    if (IsOpen) raw.sqlite3_close(_db);
		    IsOpen = false;
	    }

	    public SQLiteStatement CreateStatement(string sql)
	    {
			Logger?.WriteLine(sql);
		    return new SQLiteStatement(_db, sql);
	    }

	    public void Execute(string sql)
	    {
		    using (var statement = CreateStatement(sql))
		    {
			    statement.Execute();
		    }
		}

	    public void Execute(string sql, params object[] bindings)
	    {
		    using (var statement = CreateStatement(sql))
		    {
			    for (int i = 0; i < bindings.Length; i++)
			    {
				    statement.Bind(i + 1, bindings[i]);
			    }
			    statement.Execute();
		    }
		}

	    public T ExecuteScalar<T>(string sql)
	    {
			using (var query = ExecuteQuery(sql))
			{
				query.Step();
				return (T) Convert.ChangeType(query[0], typeof(T));
			}
		}

	    public T ExecuteScalar<T>(string sql, params object[] bindings)
	    {
		    using (var query = ExecuteQuery(sql, bindings))
		    {
			    query.Step();
			    return (T)Convert.ChangeType(query[0], typeof(T));
		    }
	    }

		public SQLiteQuery ExecuteQuery(string sql)
		{
			Logger?.WriteLine(sql);
			return new SQLiteQuery(_db, sql);
	    }

		public SQLiteQuery ExecuteQuery(string sql, params object[] bindings)
	    {
			SQLiteQuery query = new SQLiteQuery(_db, sql);
		    for (int i = 0; i < bindings.Length; i++)
		    {
			    query.Bind(i + 1, bindings[i]);
		    }
		    return query;
		}

	    public IQueryable<T> Query<T>(string sql)
	    {
			return new Query<T>(
			    _provider,
			    new ProjectionExpression(
				    new RawQueryExpression(typeof(IQueryable<T>), null, sql),
					Expression.MemberInit(
						Expression.New(typeof(T)),
						typeof(T).GetRuntimeProperties()
						.Where(pi => pi.GetCustomAttribute<IgnoreAttribute>() == null)
						.Select(pi => Expression.Bind(
							pi,
							new ColumnExpression(
								pi.PropertyType,
								null,
								pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? pi.Name
							)
						))
					)
				)
		    );
	    }

		public IQueryable<T> Query<T>(string sql, Expression<Func<IFieldReader, T>> selector)
	    {
		    return new Query<T>(
			    _provider,
			    new ProjectionExpression(
				    new RawQueryExpression(typeof(IQueryable<T>), null, sql),
				    FieldReaderReplacer.Replace(null, selector.Body)
			    )
		    );
	    }
		
	    private static IEnumerable<PropertyInfo> GetSelectedColumns(Expression expression)
	    {
		    if (expression is MemberInitExpression memberInit)
		    {
			    return memberInit.Bindings.Cast<MemberAssignment>()
				    .Select(ma => ma.Expression).Cast<MemberExpression>()
				    .Select(me => me.Member).Cast<PropertyInfo>();
		    }
		    else if (expression is NewExpression newExpression)
		    {
			    return newExpression.Arguments.Cast<MemberExpression>()
				    .Select(me => me.Member).Cast<PropertyInfo>();
			}
			else if (expression is MemberExpression member)
		    {
			    return Enumerable.Repeat((PropertyInfo) member.Member, 1);
		    }
		    else if (expression is LambdaExpression lambda)
		    {
			    return GetSelectedColumns(lambda.Body);
		    }
		    else
		    {
			    throw new ArgumentOutOfRangeException();
		    }
		}

	    public void CreateTable<TColumns>(string name, Func<ColumnBuilder, TColumns> columns)
	    {
		    CreateTable(name, columns, builder => new {});
	    }
		
		public void CreateTable<TColumns, TConstraints>(string name, Func<ColumnBuilder, TColumns> columns, Expression<Func<ITableBuilder<TColumns>, TConstraints>> constraints)
	    {
			Dictionary<PropertyInfo, ColumnModel> columnModelMap = new Dictionary<PropertyInfo, ColumnModel>();
			List<ColumnModel> columnModels = new List<ColumnModel>();

		    var tableColumns = columns(new ColumnBuilder());
		    foreach (var pi in typeof(TColumns).GetRuntimeProperties())
		    {
				ColumnModel model = (ColumnModel) pi.GetValue(tableColumns);

			    model.Name = model.Name ?? pi.Name;

				columnModels.Add(model);
			    columnModelMap[pi] = model;
		    }

			List<string> tableConstraints = new List<string>();
		    if (constraints != null)
		    {
				NewExpression constraintsInit = (NewExpression) constraints.Body;
			    for (int i = 0; i < constraintsInit.Arguments.Count; i++)
			    {
				    MethodCallExpression methodCall = (MethodCallExpression) constraintsInit.Arguments[i];
					var args = methodCall.Arguments;

					string constraint = $"CONSTRAINT [{constraintsInit.Members[i].Name}] ";

					switch (methodCall.Method.Name)
					{
						case nameof(ITableBuilder<TConstraints>.PrimaryKey):
							constraint += "PRIMARY KEY([";
							constraint += string.Join("], [", GetSelectedColumns(args[0]).Select(p => columnModelMap[p].Name));
							constraint += "])";
							break;
						case nameof(ITableBuilder<TConstraints>.Unique):
							constraint += "UNIQUE([";
							constraint += string.Join("], [", GetSelectedColumns(args[0]).Select(p => columnModelMap[p].Name));
							constraint += "])";
							break;
						case nameof(ITableBuilder<TConstraints>.ForeignKey):
							constraint += "FOREIGN KEY([";
							constraint += string.Join("], [", GetSelectedColumns(args[0]).Select(p => columnModelMap[p].Name));
							constraint += $"]) REFERENCES [{((ConstantExpression) args[1]).Value}]";
							if (args[2] is NewArrayExpression newArray && newArray.Expressions.Count > 0)
							{
								constraint += "([";
								constraint += string.Join("], [", newArray.Expressions.Cast<ConstantExpression>().Select(c => c.Value).Cast<string>());
								constraint += "])";
							}
							break;
					}

					tableConstraints.Add(constraint);
				}
		    }

		    // Build create statement.
		    StringBuilder sb = new StringBuilder();

		    sb.Append("CREATE TABLE [").Append(name).Append("]").AppendLine();
		    sb.Append("(").AppendLine();
		    {
			    foreach (var model in columnModels)
			    {
				    sb.Append('[').Append(model.Name).Append(']').Append(" ").Append(model.Type.ToString().ToUpper());
				    if (model.PrimaryKey)
				    {
					    sb.Append(" PRIMARY KEY");
					    if (model.AutoIncrement)
					    {
						    sb.Append(" AUTOINCREMENT");
					    }
				    }
				    if (!model.Nullable) sb.Append(" NOT NULL");

				    sb.AppendLine(", ");
			    }

			    foreach (var constraint in tableConstraints)
			    {
				    sb.Append(constraint).AppendLine(", ");
			    }

			    sb.Remove(sb.Length - 4, 4).AppendLine();
		    }
		    sb.Append(");").AppendLine();

		    Execute(sb.ToString());
	    }

		public void CreateView<T>(string name, IQueryable<T> query, bool temp = false)
	    {
		    StringBuilder sb = new StringBuilder();
		    sb.Append("CREATE ");
		    if (temp) sb.Append("TEMP ");
		    sb.Append("VIEW [").Append(name).Append("] AS ");

		    ProjectionExpression projection = (ProjectionExpression) _provider.Translate(query.Expression);
		    if (projection.Aggregator != null || !ProjectionIsSimple(projection.Projector))
		    {
			    throw new ArgumentException("The query cannot completely converted to SQL.");
		    }
		    
		    sb.Append(QueryFormatter.Format(projection.Source));
		    sb.Append(";");

			Execute(sb.ToString());
		}

	    private static bool ProjectionIsSimple(Expression projector)
	    {
			switch (projector)
			{
				case ColumnExpression c:
				case NewExpression newExpression when newExpression.Arguments.All(e => e is ColumnExpression):
					return true;
				case MemberInitExpression memberInit:
					if (memberInit.Bindings.Cast<MemberAssignment>().All(ma => ma.Expression is ColumnExpression) &&
						memberInit.NewExpression.Arguments.All(e => e is ColumnExpression))
					{
						return true;
					}
					goto default;
				default:
					return false;
			}
		}

	    public void CreateIndex<T>(string name, bool unique, Expression<Func<T, object>> columns)
	    {
		    string tableName = typeof(T).GetTypeInfo().GetCustomAttribute<TableAttribute>()?.Name ?? typeof(T).Name;
		    CreateIndex(name, unique, tableName, columns);
	    }
		
		public void CreateIndex<T>(string name, bool unique, string tableName, Expression<Func<T, object>> columns)
	    {
			CreateIndex(
				name, unique, tableName,
				GetSelectedColumns(columns.Body)
				.Select(pi => pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? pi.Name)
				.ToArray()
			);
		}

	    public void CreateIndex(string name, bool unique, string tableName, string[] columns)
	    {
		    StringBuilder sb = new StringBuilder();
		    sb.Append("CREATE ");
		    if (unique) sb.Append("UNIQUE ");
		    sb.Append("INDEX [").Append(name).Append("] ");
		    sb.Append("ON [").Append(tableName).Append("] ");
		    sb.Append("(");
		    {
			    for (int i = 0; i < columns.Length; i++)
			    {
				    if (i > 0) sb.Append(", ");

				    sb.Append(columns[i]);
			    }
		    }
		    sb.Append(");");

			Execute(sb.ToString());
	    }

		public void CreateTrigger(string name, bool temp = false)
	    {
		    
	    }

	    public void DropTable(string name, bool ifExists = false)
	    {
		    if (ifExists)
		    {
				Execute($"DROP TABLE IF EXISTS [{name}];");
		    }
		    else
		    {
				Execute($"DROP TABLE [{name}];");
			}
	    }

	    public void DropView(string name, bool ifExists = false)
	    {
			if (ifExists)
			{
				Execute($"DROP VIEW IF EXISTS [{name}];");
			}
			else
			{
				Execute($"DROP VIEW [{name}];");
			}
		}

	    public void DropIndex(string name, bool ifExists = false)
	    {
			if (ifExists)
			{
				Execute($"DROP INDEX IF EXISTS [{name}];");
			}
			else
			{
				Execute($"DROP INDEX [{name}];");
			}
		}

	    public void DropTrigger(string name, bool ifExists = false)
	    {
			if (ifExists)
			{
				Execute($"DROP TRIGGER IF EXISTS [{name}];");
			}
			else
			{
				Execute($"DROP TRIGGER [{name}];");
			}
		}

	    public void BeginTransaction(TransactionType? type = null)
		{
			switch (type)
			{
				case null:
					Execute("BEGIN TRANSACTION");
					break;
				case TransactionType.Deferred:
					Execute("BEGIN DEFERRED TRANSACTION");
					break;
				case TransactionType.Immediate:
					Execute("BEGIN IMMEDIATE TRANSACTION");
					break;
				case TransactionType.Exclusive:
					Execute("BEGIN EXCLUSIVE TRANSACTION");
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(type), type, null);
			}
			_transactionStack.Push(null);
	    }

	    public void EndTransaction()
	    {
			_transactionStack.Clear();
		    Execute("END TRANSACTION");
	    }

	    public void RollbackTransaction()
	    {
			_transactionStack.Clear();
			Execute("ROLLBACK TRANSACTION");
		}

	    public string SavePoint(string savePoint = null)
	    {
			if(savePoint == null) savePoint = $"tsp{TransactionDepth}";
			_transactionStack.Push(savePoint);
		    Execute($"SAVEPOINT {savePoint}");
		    return savePoint;
	    }

	    public void ReleaseSavePoint(string savePoint)
		{
			if (!_transactionStack.Contains(savePoint))
				throw new ArgumentOutOfRangeException(nameof(savePoint), savePoint, "Savepoint does not exist");

			while (savePoint != _transactionStack.Pop()){}

		    Execute($"RELEASE SAVEPOINT {savePoint}");
	    }

	    public void RollbackTo(string savePoint)
		{
			if(!_transactionStack.Contains(savePoint))
				throw new ArgumentOutOfRangeException(nameof(savePoint), savePoint, "Savepoint does not exist");

			while (savePoint != _transactionStack.Pop()) { }

			Execute($"ROLLBACK TRANSACTION TO SAVEPOINT {savePoint}");
		}

	    public void RunInTransaction(Action action)
	    {
		    string savePoint = SavePoint();
		    try
		    {
			    action();
				ReleaseSavePoint(savePoint);
		    }
		    catch (Exception)
		    {
				RollbackTo(savePoint);
			    throw;
		    }
	    }
    }
}
