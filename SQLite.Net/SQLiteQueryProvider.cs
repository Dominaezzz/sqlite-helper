using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;
using SQLite.Net.Translation;

namespace SQLite.Net
{
	public class SQLiteQueryProvider : IQueryProvider
	{
		private static readonly MethodInfo ExecuteQueryMethod = typeof(SQLiteQueryProvider).GetTypeInfo()
			.DeclaredMethods.Single(mi => mi.Name == nameof(ExecuteQuery));

		public SQLiteDatabase Database { get; }

		public SQLiteQueryProvider(SQLiteDatabase db)
		{
			Database = db;
		}

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
		{
			return new Query<TElement>(this, expression);
		}

		public IQueryable CreateQuery(Expression expression)
		{
			Type elementType = TypeSystem.GetElementType(expression.Type);
			try
			{
				return (IQueryable)Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), this, expression);
			}
			catch (TargetInvocationException tie)
			{
				throw tie.InnerException;
			}
		}

		public object Execute(Expression expression)
		{
			var elementType = TypeSystem.GetElementType(expression.Type);
			var projection = (ProjectionExpression) Translate(expression);
			
			var sequence = ExecuteQueryMethod.MakeGenericMethod(elementType)
				.Invoke(this, new object[] { projection });

			if (projection.Aggregator != null)
			{
				var aggregator = projection.Aggregator.Compile();
				try
				{
					return aggregator.DynamicInvoke(sequence);
				}
				catch(TargetInvocationException tie)
				{
					throw tie.InnerException;
				}
			}
			return sequence;
		}

		public TResult Execute<TResult>(Expression expression)
		{
			return (TResult)Execute(expression);
		}

		internal Expression Translate(Expression expression)
		{
			if (expression is ProjectionExpression projection) return projection;
			expression = Evaluator.PartialEval(expression, CanBeEvaluatedLocally);
			expression = ConstantEscaper.EscapeConstants(expression);
			expression = QueryBinder.Bind(this, expression);
			expression = OrderByRewriter.Rewrite(expression);
			expression = RedundantSubqueryRemover.Remove(expression);
			expression = UnusedColumnRemover.Remove(expression);
			expression = AggregateSimplifier.Simplify(expression);
			return expression;
		}

		private bool CanBeEvaluatedLocally(Expression expression)
		{
			// any operation on a query can't be done locally
			if (expression is ConstantExpression cs && cs.Value is IQueryable query && query.Provider == this)
			{
				return false;
			}
			if (expression is AliasedExpression)
			{
				return false;
			}
			if (expression.NodeType >= (ExpressionType) DbExpressionType.Table)
			{
				return false;
			}
			return expression.NodeType != ExpressionType.Parameter && expression.NodeType != ExpressionType.Lambda;
		}

		private IEnumerable<T> ExecuteQuery<T>(ProjectionExpression projection)
		{
			List<object> args = new List<object>();
			string sql = QueryFormatter.Format(projection.Source, args);

			using (SQLiteQuery query = Database.ExecuteQuery(sql, args.ToArray()))
			{
				var projectorExpr = ProjectionBuilder.Build(projection.Projector, projection.Source.Alias, query);

				Func<ProjectionRow, T> projector = (Func<ProjectionRow, T>)projectorExpr.Compile();
				ProjectionRow projectionRow = new SQLiteProjectionRow(this, query);

				while (query.Step())
				{
					T current = projector(projectionRow);
					yield return current;
				}
			}
		}

		private class SQLiteProjectionRow : ProjectionRow
		{
			private readonly SQLiteQueryProvider _provider;
			private readonly SQLiteQuery _query;

			public SQLiteProjectionRow(SQLiteQueryProvider provider, SQLiteQuery query)
			{
				_provider = provider;
				_query = query;
			}

			public override object GetValue(int index)
			{
				if (index < 0) throw new IndexOutOfRangeException();

				return _query[index];
			}

			public override long GetLong(int index)
			{
				return _query.GetLong(index);
			}

			public override int GetInt(int index)
			{
				return _query.GetInt(index);
			}

			public override double GetDouble(int index)
			{
				return _query.GetDouble(index);
			}

			public override string GetText(int index)
			{
				return _query.GetText(index);
			}

			public override byte[] GetBlob(int index)
			{
				return _query.GetBlob(index);
			}

			public override bool IsNull(int index)
			{
				return _query.IsNull(index);
			}

			public override IEnumerable<T> ExecuteSubQuery<T>(LambdaExpression query)
			{
				ProjectionExpression projection = (ProjectionExpression)new Replacer()
					.Replace(query.Body, query.Parameters[0], Expression.Constant(this, typeof(ProjectionRow)));

				projection = (ProjectionExpression)Evaluator.PartialEval(projection, CanEvaluateLocally);

				IEnumerable<T> result = _provider.ExecuteQuery<T>(projection);
				
				if (typeof(IQueryable<T>).GetTypeInfo().IsAssignableFrom(query.Body.Type.GetTypeInfo()))
				{
					return result.AsQueryable();
				}
				return result;
			}

			private static bool CanEvaluateLocally(Expression expression)
			{
				return expression.NodeType != ExpressionType.Parameter &&
					!((int)expression.NodeType >= 1000) &&
					expression.NodeType != ExpressionType.New;
			}
		}

		internal class Replacer : DbExpressionVisitor
		{
			private Expression _searchFor, _replaceWith;

			internal Expression Replace(Expression expression, Expression searchFor, Expression replaceWith)
			{
				_searchFor = searchFor;
				_replaceWith = replaceWith;

				return Visit(expression);
			}

			public override Expression Visit(Expression exp)
			{
				return exp == _searchFor ? _replaceWith : base.Visit(exp);
			}
		}
	}
}
