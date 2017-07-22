﻿using System;
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

	/// <summary>
	/// Represents a connection to an SQLite Database.
	/// </summary>
	public abstract class SQLiteDatabase : IDisposable
    {
		static SQLiteDatabase() { Batteries_V2.Init(); }

	    private readonly sqlite3 _db;
	    private readonly SQLiteQueryProvider _provider;

		/// <summary>
		/// The path of the database file this connection represents.
		/// Is null if it is an in-memory database.
		/// </summary>
		public string Path { get; }
		/// <summary>
		/// Is true if the connection to the databse is open.
		/// </summary>
		public bool IsOpen { get; private set; }
		/// <summary>
		/// A <see cref="TextWriter"/> that this connection logs to.
		/// Is not set by default.
		/// </summary>
		public TextWriter Logger { get; set; }

	    private readonly Stack<string> _transactionStack = new Stack<string>();
		/// <summary>
		/// Number representing how deep in the transaction stack the database is in.
		/// </summary>
	    public int TransactionDepth => _transactionStack.Count;
		/// <summary>
		/// True if the database is currently in a transaction.
		/// </summary>
	    public bool IsInTransaction => TransactionDepth > 0;

	    public long LastInsertRowId => raw.sqlite3_last_insert_rowid(_db);
		/// <summary>
		/// <returns>
		/// The number of database rows that were changed or inserted or deleted
		/// by the most recently completed INSERT, DELETE, or UPDATE statement,
		/// exclusive of statements in lower-level triggers.
		/// </returns>
		/// </summary>
		public int Changes => raw.sqlite3_changes(_db);
		/// <summary>
		/// <returns>
		/// This function returns the total number of rows inserted,
		/// modified or deleted by all INSERT, UPDATE or DELETE statements completed
		/// since the database connection was opened,
		/// including those executed as part of trigger programs.
		/// </returns>
		/// </summary>
		public int TotalChanges => raw.sqlite3_total_changes(_db);

	    public TimeSpan BusyTimeout
		{
		    get => TimeSpan.FromMilliseconds(ExecuteScalar<long>("PRAGMA busy_timeout;"));
			set => raw.sqlite3_busy_timeout(_db, (int) value.TotalMilliseconds);
	    }
		/// <summary>
		/// The user-version is an integer that is available to applications to use however they want.
		/// SQLite makes no use of the user-version itself.
		/// 
		/// It is usually used to keep track of migrations.
		/// It's initial value is 0.
		/// </summary>
		public long UserVersion
	    {
		    get => ExecuteScalar<long>("PRAGMA user_version");
		    set => Execute($"PRAGMA user_version = {value}");
	    }
		/// <summary>
		/// Query, set, or clear the enforcement of foreign key constraints.
		/// 
	    /// This pragma is a no-op within a transaction;
	    /// foreign key constraint enforcement may only be enabled or disabled when there is no pending BEGIN or SAVEPOINT.
		/// </summary>
		public bool ForeignKeysEnabled
	    {
			get => ExecuteScalar<bool>("PRAGMA foreign_keys");
		    set => Execute($"PRAGMA foreign_keys = {value}");
		}

		/// <summary>
		/// <para>
		/// Every SQLite database has an SQLITE_MASTER table that defines the schema for the database.
		/// </para>
		/// <para>
		///	For tables, the type field will always be 'table' and the name field will be the name of the table.
		///	For indices, type is equal to 'index', name is the name of the index and tbl_name is the name of the table to which the index belongs.
		/// For both tables and indices, the sql field is the text of the original CREATE TABLE or CREATE INDEX statement that created the table or index.
		/// For automatically created indices (used to implement the PRIMARY KEY or UNIQUE constraints) the sql field is NULL.
		/// </para>
		/// <para>
		/// The SQLITE_MASTER table is read-only.
		/// You cannot change this table using UPDATE, INSERT, or DELETE.
		/// The table is automatically updated by CREATE TABLE, CREATE INDEX, DROP TABLE, and DROP INDEX commands.
		/// </para>
		/// </summary>
		/// 
		/// <example>
		/// So to get a list of all tables in the database, use the following SELECT command:
		///	<code>
		/// SQLiteMaster.Where(o => o.Type == "table").OrderBy(o => o.Name).Select(o => o.Name)
		/// </code>
		/// </example>
		public Table<DatabaseObject> SQLiteMaster { get; set; }
		/// <summary>
		/// Temporary tables do not appear in the SQLITE_MASTER table.
		/// Temporary tables and their indices and triggers occur in another special table named SQLITE_TEMP_MASTER.
		/// <para>
		/// SQLITE_TEMP_MASTER works just like SQLITE_MASTER,
		/// except that it is only visible to the application that created the temporary tables.
		/// </para>
		/// </summary>
		public Table<DatabaseObject> SQLiteTempMaster { get; set; }

		/// <summary>
		/// Creates a database connection to the file at the path specified or creates an in-memory database if path is null.
		/// <para>
		/// Initializes all <see cref="Table{T}"/> and <see cref="View{T}"/> properties with their correct names.
		/// If the model class has no <see cref="TableAttribute"/> then the name of the property is used,
		/// not the name of the type.
		/// </para>
		/// </summary>
		/// <param name="path">
		/// The path of the database file.
		/// Null if an in-memory database is desired.
		/// </param>
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

		/// <summary>
		/// Closes the database connection if it is open.
		/// </summary>
		public void Dispose()
	    {
		    if (IsOpen) raw.sqlite3_close(_db);
		    IsOpen = false;
	    }

		/// <summary>
		/// Creates a re-usable <see cref="SQLiteStatement"/>, initialized with the given sql statement.
		/// </summary>
		/// <param name="sql">The sql statement to initialize the object with.</param>
		/// <returns>A re-usable <see cref="SQLiteStatement"/></returns>
		public SQLiteStatement CreateStatement(string sql)
	    {
			Logger?.WriteLine(sql);
		    return new SQLiteStatement(_db, sql);
	    }

		/// <summary>
		/// Executes the given sql statement.
		/// </summary>
		/// <param name="sql">SQL statement to execute.</param>
	    public void Execute(string sql)
	    {
		    using (var statement = CreateStatement(sql))
		    {
			    statement.Execute();
		    }
		}

		/// <summary>
		/// Executes the given sql statement. Don't use this if you want data returned.
		/// </summary>
		/// <param name="sql">SQL statement to execute.</param>
		/// <param name="bindings">You may include ?s in the sql statement, which will be replaced by the values from bindings.</param>
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

		/// <summary>
		/// Executes the given sql statement and returns the value at the first row and first column.
		/// <para>Should return a 1x1 result.</para>
		/// </summary>
		/// <typeparam name="T">The type to return the result as.</typeparam>
		/// <param name="sql">SQL statement to execute.</param>
		/// <returns>The value at the first row and first column.</returns>
		public T ExecuteScalar<T>(string sql)
	    {
			using (var query = ExecuteQuery(sql))
			{
				query.Step();
				return (T) Convert.ChangeType(query[0], typeof(T));
			}
		}

		/// <summary>
		/// Executes the given sql statement and returns the value at the first row and first column.
		/// <para>Should return a 1x1 result.</para>
		/// </summary>
		/// <typeparam name="T">The type to return the result as.</typeparam>
		/// <param name="sql">SQL statement to execute.</param>
		/// <param name="bindings">You may include ?s in the sql statement, which will be replaced by the values from bindings.</param>
		/// <returns>The value at the first row and first column.</returns>
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

		/// <summary>
		/// Creates an <see cref="IQueryable{T}"/> for the query given.
		/// <para>The returned <see cref="IQueryable{T}"/> can be filtered and have other LINQ operations applied to it.</para>
		/// <para>This is limited to public properties only, which have to match the columns returned from the query.</para>
		/// </summary>
		/// <typeparam name="T">The element type of the <see cref="IQueryable{T}"/></typeparam>
		/// <param name="sql">The query to be executed.</param>
		/// <returns></returns>
		public IQueryable<T> Query<T>(string sql)
	    {
			return new Query<T>(
			    _provider,
			    new ProjectionExpression(
				    new RawQueryExpression(typeof(IQueryable<T>), null, sql),
					Expression.MemberInit(
						Expression.New(typeof(T)),
						typeof(T).GetRuntimeProperties()
						.Where(pi => pi.IsDefined(typeof(IgnoreAttribute)))
						.Select(pi => Expression.Bind(
							pi,
							new ColumnExpression(
								pi.PropertyType,
								null,
								Orm.GetColumnName(pi)
							)
						))
					)
				)
		    );
	    }

		/// <summary>
		/// Creates an <see cref="IQueryable{T}"/> for the query given.
		/// <para>The returned <see cref="IQueryable{T}"/> can be filtered and have other LINQ operations applied to it.</para>
		/// <para></para>
		/// </summary>
		/// <typeparam name="T">The element type of the <see cref="IQueryable{T}"/></typeparam>
		/// <param name="sql">The query to be executed.</param>
		/// <param name="selector">A custom projector to return anything you want from each row.</param>
		/// <returns></returns>
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

		/// <summary>
		/// Creates a view with the given name and query.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="name">The name of the view.</param>
		/// <param name="query">The query to store in the view.</param>
		/// <param name="temp">If true the view will only exist for this connection.</param>
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

		/// <summary>
		/// Creates an index with the given name and columns.
		/// </summary>
		/// <param name="name">A name to give the index.</param>
		/// <param name="unique">True if the index should be declared unique.</param>
		/// <param name="tableName">The name of the table to index.</param>
		/// <param name="columns">The columns in the named table to index.</param>
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

		/// <summary>
		/// Creates a trigger.
		/// METHOD NOT YET IMPLEMENTED!!!
		/// </summary>
		/// <param name="name"></param>
		/// <param name="temp"></param>
		public void CreateTrigger(string name, bool temp = false)
	    {
		    throw new NotImplementedException();
	    }

		/// <summary>
		/// Drops the table with the given name.
		/// </summary>
		/// <param name="name">The name of the table.</param>
		/// <param name="ifExists">Adds the 'IF EXISTS' clause to the command.</param>
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

		/// <summary>
		/// Drops the view with the given name.
		/// </summary>
		/// <param name="name">The name of the view.</param>
		/// <param name="ifExists">Adds the 'IF EXISTS' clause to the command.</param>
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

		/// <summary>
		/// Drops the index with the given name.
		/// </summary>
		/// <param name="name">The name of the index.</param>
		/// <param name="ifExists">Adds the 'IF EXISTS' clause to the command.</param>
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

		/// <summary>
		/// Drops the trigger with the given name.
		/// </summary>
		/// <param name="name">The name of the trigger.</param>
		/// <param name="ifExists">Adds the 'IF EXISTS' clause to the command.</param>
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

		/// <summary>
		/// <para>
		/// Transactions can be deferred, immediate, or exclusive.
		/// The default transaction behavior is deferred.
		/// </para>
		/// Deferred means that no locks are acquired on the database until the database is first accessed.
		/// Thus with a deferred transaction, the BEGIN statement itself does nothing to the filesystem.
		/// Locks are not acquired until the first read or write operation.
		/// The first read operation against a database creates a SHARED lock and the first write operation creates a RESERVED lock.
		/// Because the acquisition of locks is deferred until they are needed, it is possible that another thread or process could create a separate transaction and write to the database after the BEGIN on the current thread has executed.
		/// <para>
		/// If the transaction is immediate, then RESERVED locks are acquired on all databases as soon as the BEGIN command is executed, without waiting for the database to be used.
		/// After a BEGIN IMMEDIATE, no other database connection will be able to write to the database or do a BEGIN IMMEDIATE or BEGIN EXCLUSIVE.
		/// Other processes can continue to read from the database, however.
		/// </para>
		/// <para>
		/// An exclusive transaction causes EXCLUSIVE locks to be acquired on all databases.
		/// After a BEGIN EXCLUSIVE, no other database connection except for read_uncommitted connections will be able to read the database
		/// and no other connection without exception will be able to write the database until the transaction is complete.
		/// </para>
		/// </summary>
		/// <param name="type"></param>
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

		/// <summary>
		/// Commits the pending transaction.
		/// </summary>
	    public void EndTransaction()
	    {
			_transactionStack.Clear();
		    Execute("END TRANSACTION");
	    }

		/// <summary>
		/// Rollback the pending transaction, so all changes made since the last <see cref="BeginTransaction"/> are lost.
		/// </summary>
	    public void RollbackTransaction()
	    {
			_transactionStack.Clear();
			Execute("ROLLBACK TRANSACTION");
		}

		/// <summary>
		/// Creates a savepoint in the database at the current point in the transaction timeline.
		/// Begins a new transaction if one is not in progress.
		/// 
		/// Call <see cref="RollbackTo"/> to undo transactions since the returned savepoint.
		/// Call <see cref="ReleaseSavePoint"/> to commit transactions after the savepoint returned here.
		/// Call <see cref="EndTransaction"/> to end the transaction, committing all changes.
		/// </summary>
		/// <param name="savePoint">An optional string to name the save point.</param>
		/// <returns>A string representing the save point.</returns>
		public string SavePoint(string savePoint = null)
	    {
			if(savePoint == null) savePoint = $"tsp{TransactionDepth}";
			_transactionStack.Push(savePoint);
		    Execute($"SAVEPOINT {savePoint}");
		    return savePoint;
	    }

		/// <summary>
		/// Commits all changes made since the savepoint was created to the outer transaction.
		/// This does not mean the changes are written to the database, unless this save point was the outer most one.
		/// </summary>
		/// <param name="savePoint">The label of the save point to 'commit'.</param>
	    public void ReleaseSavePoint(string savePoint)
		{
			if (!_transactionStack.Contains(savePoint))
				throw new ArgumentOutOfRangeException(nameof(savePoint), savePoint, "Savepoint does not exist");

			while (savePoint != _transactionStack.Pop()){}

		    Execute($"RELEASE SAVEPOINT {savePoint}");
	    }

		/// <summary>
		/// Reverts all changes made since the savepoint was created.
		/// </summary>
		/// <param name="savePoint">The label of the save point to rollback.</param>
		public void RollbackTo(string savePoint)
		{
			if(!_transactionStack.Contains(savePoint))
				throw new ArgumentOutOfRangeException(nameof(savePoint), savePoint, "Savepoint does not exist");

			while (savePoint != _transactionStack.Pop()) { }

			Execute($"ROLLBACK TRANSACTION TO SAVEPOINT {savePoint}");
		}

		/// <summary>
		/// Runs the given action in a transaction.
		/// This method can be nested.
		/// </summary>
		/// <param name="action">The action to be executed.</param>
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
