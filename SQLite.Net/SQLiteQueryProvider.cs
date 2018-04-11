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
	internal class SQLiteQueryProvider : IQueryProvider
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
			expression = QueryBinder.Bind(this, expression);
			expression = ConstantEscaper.EscapeConstants(expression);
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
			return expression.NodeType != ExpressionType.Parameter && expression.NodeType != ExpressionType.Lambda &&
				expression.NodeType != ExpressionType.New;
		}

		internal IEnumerable<T> ExecuteQuery<T>(ProjectionExpression projection)
		{
			List<object> args = new List<object>();
			string sql = QueryFormatter.Format(projection.Source, args);
			Database.Log?.Invoke(sql);

			using (SQLiteQuery query = Database.ExecuteQuery(sql, args.ToArray()))
			{
				LambdaExpression projectorExpr = ProjectionBuilder.Build(
					projection.Projector,
					projection.Source.Alias,
					name => string.IsNullOrEmpty(name) ? 0 : query.GetColumnIndex(name)
				);
				
				var projector = (Func<SQLiteQueryProvider, SQLiteQuery, T>)projectorExpr.Compile();

				while (query.Step())
				{
					yield return projector(this, query);
				}
			}
		}
	}
}
