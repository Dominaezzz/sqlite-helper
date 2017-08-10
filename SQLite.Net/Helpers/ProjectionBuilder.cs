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
	/// <summary>
	/// A ProjectionRow is an abstract over a row based data source
	/// </summary>
	internal class ProjectionRow
	{
		private readonly SQLiteQueryProvider _provider;
		private readonly SQLiteQuery _query;

		public ProjectionRow(SQLiteQueryProvider provider, SQLiteQuery query)
		{
			_provider = provider;
			_query = query;
		}

		public object GetValue(int index)
		{
			if (index < 0) throw new IndexOutOfRangeException();

			return _query[index];
		}

		public long GetLong(int index)
		{
			return _query.GetLong(index);
		}

		public int GetInt(int index)
		{
			return _query.GetInt(index);
		}

		public double GetDouble(int index)
		{
			return _query.GetDouble(index);
		}

		public string GetText(int index)
		{
			return _query.GetText(index);
		}

		public byte[] GetBlob(int index)
		{
			return _query.GetBlob(index);
		}

		public bool IsNull(int index)
		{
			return _query.IsNull(index);
		}

		public IEnumerable<T> ExecuteSubQuery<T>(LambdaExpression query)
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

	/// <summary>
	/// ProjectionBuilder is a visitor that converts an projector expression
	/// that constructs result objects out of ColumnExpressions into an actual
	/// LambdaExpression that constructs result objects out of accessing fields
	/// of a ProjectionRow
	/// </summary>
	internal class ProjectionBuilder : DbExpressionVisitor
	{
		private static readonly MethodInfo MiGetValue = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetValue), new []{ typeof(int) });
		private static readonly MethodInfo MiGetLong = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetLong), new[] { typeof(int) });
		private static readonly MethodInfo MiGetInt = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetInt), new[] { typeof(int) });
		private static readonly MethodInfo MiGetDouble = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetDouble), new[] { typeof(int) });
		private static readonly MethodInfo MiGetText = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetText), new[] { typeof(int) });
		private static readonly MethodInfo MiGetBlob = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.GetBlob), new[] { typeof(int) });
		private static readonly MethodInfo MiIsNull = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.IsNull), new[] { typeof(int) });
		private static readonly MethodInfo MiExecuteSubQuery = typeof(ProjectionRow).GetRuntimeMethod(nameof(ProjectionRow.ExecuteSubQuery), new []{ typeof(LambdaExpression) });
		private readonly ParameterExpression _row = Expression.Parameter(typeof(ProjectionRow), "row");
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
			Expression body = builder.Visit(expression);
			return Expression.Lambda(body, builder._row);
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
			LambdaExpression subQuery = Expression.Lambda(base.VisitProjection(proj), _row);
			Type elementType = TypeSystem.GetElementType(subQuery.Body.Type);
			MethodInfo mi = MiExecuteSubQuery.MakeGenericMethod(elementType);
			return Expression.Convert(
				Expression.Call(_row, mi, Expression.Constant(subQuery)),
				proj.Type
			);
		}
	}
}
