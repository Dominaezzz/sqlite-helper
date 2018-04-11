using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SQLite.Net.Attributes;
using SQLite.Net.Expressions;

namespace SQLite.Net.Helpers
{
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

	/// <summary>
	/// ProjectionBuilder is a visitor that converts an projector expression
	/// that constructs result objects out of ColumnExpressions into an actual
	/// LambdaExpression that constructs result objects out of accessing fields
	/// of a ProjectionRow
	/// </summary>
	internal class ProjectionBuilder : DbExpressionVisitor
	{
		private static readonly MethodInfo MiGetValue = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetValue), new []{ typeof(int) });
		private static readonly MethodInfo MiGetLong = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetLong), new[] { typeof(int) });
		private static readonly MethodInfo MiGetInt = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetInt), new[] { typeof(int) });
		private static readonly MethodInfo MiGetDouble = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetDouble), new[] { typeof(int) });
		private static readonly MethodInfo MiGetText = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetText), new[] { typeof(int) });
		private static readonly MethodInfo MiGetBlob = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.GetBlob), new[] { typeof(int) });
		private static readonly MethodInfo MiIsNull = typeof(SQLiteQuery).GetRuntimeMethod(nameof(SQLiteQuery.IsNull), new[] { typeof(int) });
		private static readonly MethodInfo MiExecuteSubQuery = typeof(ProjectionBuilder).GetRuntimeMethod(nameof(ExecuteSubQuery), new []{ typeof(SQLiteQueryProvider), typeof(SQLiteQuery), typeof(LambdaExpression) });
		private readonly ParameterExpression _row = Expression.Parameter(typeof(SQLiteQuery), "row");
		private readonly ParameterExpression _provider = Expression.Parameter(typeof(SQLiteQueryProvider), "_provider");
		private readonly string _rowAlias;
		private readonly Func<string, int> _getColumnIndex;

		private ProjectionBuilder(string rowAlias, Func<string, int> getColumnIndex)
		{
			_rowAlias = rowAlias;
			_getColumnIndex = getColumnIndex;
		}

		internal static LambdaExpression Build(Expression expression, string alias, Func<string, int> getColumnIndex)
		{
			ProjectionBuilder builder = new ProjectionBuilder(alias, getColumnIndex);
			return Expression.Lambda(builder.Visit(expression), builder._provider, builder._row);
		}

		protected override Expression VisitColumn(ColumnExpression column)
		{
			if (column.Alias == _rowAlias)
			{
				int iOrdinal = _getColumnIndex(column.Name);
				Expression index = Expression.Constant(iOrdinal);

				var clrType = Nullable.GetUnderlyingType(column.Type) ?? column.Type;

				Expression result;

				if (Orm.IntegralTypes.Contains(clrType))
				{
					result = Expression.Convert(Expression.Call(_row, MiGetLong, index), clrType);
				}
				else if (Orm.FractionalTypes.Contains(clrType))
				{
					result = Expression.Convert(Expression.Call(_row, MiGetDouble, index), clrType);
				}
				else if (clrType == typeof(string))
				{
					result = Expression.Call(_row, MiGetText, index);
				}
				else if (clrType == typeof(char))
				{
					result = Expression.MakeIndex(
						Expression.Call(_row, MiGetText, index),
						typeof(string).GetTypeInfo().DeclaredProperties.Single(p => p.GetIndexParameters().Length > 0),
						Enumerable.Repeat(Expression.Constant(0), 1)
					);
				}
				else if (clrType == typeof(byte[]))
				{
					result = Expression.Call(_row, MiGetBlob, index);
				}
				else if (clrType == typeof(bool))
				{
					result = Expression.Equal(Expression.Call(_row, MiGetInt, index), Expression.Constant(1));
				}
				else if (clrType == typeof(TimeSpan))
				{
					result = Expression.Call(
						typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
						Expression.Call(_row, MiGetLong, index)
					);
				}
				else if (clrType == typeof(DateTime))
				{
					result = Expression.Call(
						typeof(DateTime), nameof(DateTime.Parse), null,
						Expression.Call(_row, MiGetText, index)
					);
				}
				else if (clrType == typeof(DateTimeOffset))
				{
					result = Expression.Call(
						typeof(DateTimeOffset), nameof(DateTimeOffset.Parse), null,
						Expression.Call(_row, MiGetText, index)
					);
				}
				else if (clrType == typeof(Guid))
				{
					result = Expression.Call(
						typeof(Guid), nameof(Guid.Parse), null, Expression.Call(_row, MiGetText, index)
					);
				}
				else if (clrType.GetTypeInfo().IsEnum)
				{
					Expression fromText = Expression.Convert(
						Expression.Call(
							typeof(Enum), nameof(Enum.Parse), null,
							Expression.Constant(clrType),
							Expression.Call(_row, MiGetText, index),
							Expression.Constant(true)
						),
						clrType
					);
					if (clrType.GetTypeInfo().IsDefined(typeof(StoreAsTextAttribute)))
					{
						result = fromText;
					}
					else
					{
						var methodCall = Expression.Convert(Expression.Call(_row, MiGetInt, index), clrType);
						result = Expression.Condition(
							Expression.TypeIs(methodCall, typeof(string)), fromText, methodCall
						);
					}
				}
				else if (clrType == typeof(object))
				{
					return Expression.Call(_row, MiGetValue, index);
				}
				else
				{
					throw new NotSupportedException("Don't know how to read " + clrType);
				}

				if (result != null && clrType != column.Type) // If is nullable
				{
					result = Expression.Condition(
						Expression.Call(_row, MiIsNull, index),
						Expression.Constant(null, column.Type),
						Expression.Convert(result, column.Type)
					);
				}

				return result;
			}
			return column;
		}

		protected override Expression VisitProjection(ProjectionExpression proj)
		{
			LambdaExpression subQuery = Expression.Lambda(base.VisitProjection(proj), _provider, _row);
			Type elementType = TypeSystem.GetElementType(subQuery.Body.Type);
			MethodInfo mi = MiExecuteSubQuery.MakeGenericMethod(elementType);
			return Expression.Convert(
				Expression.Call(mi, _provider, _row, Expression.Constant(subQuery)),
				proj.Type
			);
		}

		public static IEnumerable<T> ExecuteSubQuery<T>(SQLiteQueryProvider provider, SQLiteQuery row, LambdaExpression query)
		{
			ProjectionExpression projection = (ProjectionExpression)new Replacer()
				.Replace(query.Body, query.Parameters[0], Expression.Constant(provider, typeof(SQLiteQueryProvider)));
			projection = (ProjectionExpression)new Replacer()
				.Replace(projection, query.Parameters[1], Expression.Constant(row, typeof(SQLiteQuery)));

			projection = (ProjectionExpression)Evaluator.PartialEval(projection, CanEvaluateLocally);

			IEnumerable<T> result = provider.ExecuteQuery<T>(projection);

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
}
