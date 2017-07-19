using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using SQLite.Net.Expressions;

namespace SQLite.Net.Helpers
{
	internal class DbExpressionVisitor : ExpressionVisitor
	{
		public override Expression Visit(Expression exp)
		{
			if (exp == null) return null;

			switch ((DbExpressionType)exp.NodeType)
			{
				case DbExpressionType.Table:
					return VisitTable((TableExpression)exp);
				case DbExpressionType.View:
					return VisitView((ViewExpression)exp);
				case DbExpressionType.RawQuery:
					return VisitRawQuery((RawQueryExpression)exp);
				case DbExpressionType.Join:
					return VisitJoin((JoinExpression)exp);
				case DbExpressionType.Column:
					return VisitColumn((ColumnExpression)exp);
				case DbExpressionType.Select:
					return VisitSelect((SelectExpression)exp);
				case DbExpressionType.Projection:
					return VisitProjection((ProjectionExpression)exp);
				case DbExpressionType.Aggregate:
					return VisitAggregate((AggregateExpression)exp);
				case DbExpressionType.Scalar:
				case DbExpressionType.Exists:
				case DbExpressionType.In:
					return VisitSubquery((SubQueryExpression)exp);
				case DbExpressionType.Function:
					return VisitFunction((FunctionExpression) exp);
				case DbExpressionType.TypeChange:
					return VisitTypeChange((TypeChangeExpression) exp);
				default:
					return base.Visit(exp);
			}
		}

		protected virtual Expression VisitTable(TableExpression table)
		{
			return table;
		}

		protected virtual Expression VisitView(ViewExpression view)
		{
			return view;
		}

		protected virtual Expression VisitRawQuery(RawQueryExpression rawQuery)
		{
			return rawQuery;
		}

		protected virtual Expression VisitColumn(ColumnExpression column)
		{
			return column;
		}

		protected virtual Expression VisitSelect(SelectExpression select)
		{
			Expression from = VisitSource(select.From);
			Expression where = Visit(select.Where);

			ReadOnlyCollection<ColumnDeclaration> columns = VisitColumnDeclarations(select.Columns);
			ReadOnlyCollection<Expression> groupBy = select.GroupBy == null ? null : Visit(select.GroupBy);
			Expression having = Visit(select.Having);
			ReadOnlyCollection<OrderExpression> orderBy = VisitOrderBy(select.OrderBy);
			Expression offset = Visit(select.Offset);
			Expression limit = Visit(select.Limit);

			if (from != select.From || where != select.Where || columns != select.Columns ||
			    orderBy != select.OrderBy || groupBy != select.GroupBy || having != select.Having ||
				offset != select.Offset || limit != select.Limit)
			{
				return new SelectExpression(select.Alias, columns, from, where,
					groupBy, having, orderBy, offset, limit, select.IsDistinct);
			}
			return select;
		}

		protected virtual Expression VisitSource(Expression source)
		{
			return Visit(source);
		}

		protected virtual Expression VisitProjection(ProjectionExpression proj)
		{
			QueryExpression source = (QueryExpression)Visit(proj.Source);
			Expression projector = Visit(proj.Projector);

			if (source != proj.Source || projector != proj.Projector)
			{
				return new ProjectionExpression(source, projector, proj.Aggregator);
			}
			return proj;
		}

		protected virtual Expression VisitJoin(JoinExpression join)
		{
			Expression left = Visit(join.Left);
			Expression right = Visit(join.Right);
			Expression condition = Visit(join.Condition);

			if (left != join.Left || right != join.Right || condition != join.Condition)
			{
				return new JoinExpression(join.Join, left, right, condition);
			}
			return join;
		}

		protected virtual Expression VisitAggregate(AggregateExpression aggregate)
		{
			Expression arg = Visit(aggregate.Argument);
			if (arg != aggregate.Argument)
			{
				return new AggregateExpression(aggregate.Type, aggregate.AggregateType, arg, aggregate.IsDistict);
			}
			return aggregate;
		}

		protected virtual Expression VisitSubquery(SubQueryExpression subquery)
		{
			switch (subquery.NodeType)
			{
				case (ExpressionType)DbExpressionType.Scalar:
					return VisitScalar((ScalarExpression) subquery);
				case (ExpressionType)DbExpressionType.Exists:
					return VisitExists((ExistsExpression) subquery);
				case (ExpressionType)DbExpressionType.In:
					return VisitIn((InExpression) subquery);
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		protected virtual Expression VisitScalar(ScalarExpression scalar)
		{
			SelectExpression select = (SelectExpression) Visit(scalar.Query);
			if (select != scalar.Query)
			{
				return new ScalarExpression(scalar.Type, select);
			}
			return scalar;
		}

		protected virtual Expression VisitExists(ExistsExpression exists)
		{
			SelectExpression select = (SelectExpression)Visit(exists.Query);
			if (select != exists.Query)
			{
				return new ExistsExpression(select);
			}
			return exists;
		}

		protected virtual Expression VisitIn(InExpression inExpression)
		{
			Expression expression = Visit(inExpression.Expression);
			if (inExpression.Query != null)
			{
				SelectExpression select = (SelectExpression) Visit(inExpression.Query);
				if (expression != inExpression.Expression || select != inExpression.Query)
				{
					return new InExpression(expression, select);
				}
			}
			else
			{
				ReadOnlyCollection<Expression> values = Visit(inExpression.Values);
				if (expression != inExpression.Expression || values != inExpression.Values)
				{
					return new InExpression(expression, values);
				}
			}
			return inExpression;
		}

		protected virtual Expression VisitFunction(FunctionExpression function)
		{
			var arguments = Visit(function.Arguments);
			if (arguments != function.Arguments)
			{
				return new FunctionExpression(function.Type, function.Name, arguments);
			}
			return function;
		}

		protected virtual Expression VisitTypeChange(TypeChangeExpression typeChange)
		{
			Expression expr = Visit(typeChange.Expression);
			if (expr != typeChange.Expression)
			{
				return new TypeChangeExpression(typeChange.Type, expr);
			}
			return typeChange;
		}
		
		protected ReadOnlyCollection<ColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> columns)
		{
			List<ColumnDeclaration> alternate = null;

			for (int i = 0, n = columns.Count; i < n; i++)
			{
				ColumnDeclaration column = columns[i];
				Expression e = Visit(column.Expression);

				if (alternate == null && e != column.Expression)
				{
					alternate = columns.Take(i).ToList();
				}
				alternate?.Add(new ColumnDeclaration(column.Name, e));
			}

			return alternate != null ? alternate.AsReadOnly() : columns;
		}

		protected ReadOnlyCollection<OrderExpression> VisitOrderBy(ReadOnlyCollection<OrderExpression> expressions)
		{
			if (expressions == null) return null;

			List<OrderExpression> alternate = null;

			for (int i = 0, n = expressions.Count; i < n; i++)
			{
				OrderExpression expr = expressions[i];

				Expression e = Visit(expr.Expression);

				if (alternate == null && e != expr.Expression)
				{
					alternate = expressions.Take(i).ToList();
				}
				alternate?.Add(new OrderExpression(expr.OrderType, e));
			}

			return alternate != null ? alternate.AsReadOnly() : expressions;
		}
	}
}
