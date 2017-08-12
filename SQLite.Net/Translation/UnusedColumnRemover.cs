using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal class UnusedColumnRemover : DbExpressionVisitor
	{
		private readonly Dictionary<string, HashSet<string>> _allColumnsUsed = new Dictionary<string, HashSet<string>>();

		private UnusedColumnRemover() { }

		public static Expression Remove(Expression expression)
		{
			return new UnusedColumnRemover().Visit(expression);
		}
		
		private void MarkColumnAsUsed(string alias, string name)
		{
			if (alias == null) return;
			HashSet<string> columns;
			if (!_allColumnsUsed.TryGetValue(alias, out columns))
			{
				columns = new HashSet<string>();
				_allColumnsUsed[alias] = columns;
			}
			columns.Add(name);
		}

		private bool IsColumnUsed(string alias, string name)
		{
			if (alias == null) return true;
			if (_allColumnsUsed.TryGetValue(alias, out HashSet<string> columnsUsed))
			{
				if (columnsUsed != null)
				{
					return columnsUsed.Contains(name);
				}
			}
			return false;
		}

		private void ClearColumnsUsed(string alias)
		{
			_allColumnsUsed[alias] = new HashSet<string>();
		}

		protected override Expression VisitColumn(ColumnExpression column)
		{
			MarkColumnAsUsed(column.Alias, column.Name);
			return column;
		}

		protected override Expression VisitSubquery(SubQueryExpression subquery)
		{
			if (subquery.Query is SelectExpression select)
			{
				Debug.Assert(select.Columns.Count == 1 || subquery is ExistsExpression);
				foreach (var column in select.Columns)
				{
					MarkColumnAsUsed(select.Alias, column.Name);
				}
			}
			Expression result = base.VisitSubquery(subquery);
			return result;
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			// visit column projection first
			ReadOnlyCollection<ColumnDeclaration> columns = select.Columns;

			if (_allColumnsUsed.TryGetValue(select.Alias, out HashSet<string> columnsUsed))
			{
				List<ColumnDeclaration> alternate = null;
				for (int i = 0, n = select.Columns.Count; i < n; i++)
				{
					ColumnDeclaration decl = select.Columns[i];
					if (select.IsDistinct || columnsUsed.Contains(decl.Name))
					{
						if (Visit(decl.Expression) != decl.Expression)
						{
							decl = new ColumnDeclaration(decl.Name, decl.Expression);
						}
					}
					else
					{
						decl = null; // null means it gets omitted
					}

					if (decl != select.Columns[i] && alternate == null)
					{
						alternate = select.Columns.Take(i).ToList();
					}
					if (decl != null) alternate?.Add(decl);
				}

				if (alternate != null) columns = alternate.AsReadOnly();
			}

			Expression offset = Visit(select.Offset);
			Expression limit = Visit(select.Limit);
			Expression having = Visit(select.Having);
			ReadOnlyCollection<Expression> groupBys = select.GroupBy == null ? null : Visit(select.GroupBy);
			ReadOnlyCollection<OrderExpression> orderbys = VisitOrderBy(select.OrderBy);
			Expression where = Visit(select.Where);
			Expression from = Visit(select.From);

			ClearColumnsUsed(select.Alias);

			if (columns != select.Columns || orderbys != select.OrderBy || groupBys != select.GroupBy
				|| having != select.Having || limit != select.Limit || offset != select.Offset
				|| where != select.Where || from != select.From)
			{
				return new SelectExpression(select.Alias, columns, from, where,
					groupBys, having, orderbys,
					offset, limit, select.IsDistinct);
			}
			return select;
		}

		protected override Expression VisitProjection(ProjectionExpression projection)
		{
			// visit mapping in reverse order
			// Mark all columns that are used in the projector as "used".
			Expression projector = Visit(projection.Projector);
			QueryExpression source = (QueryExpression) Visit(projection.Source);

			if (projector != projection.Projector || source != projection.Source)
			{
				return new ProjectionExpression(source, projector, projection.Aggregator);
			}
			return projection;
		}

		protected override Expression VisitJoin(JoinExpression join)
		{
			// visit join in reverse order
			Expression condition = Visit(join.Condition);
			Expression right = VisitSource(join.Right);
			Expression left = VisitSource(join.Left);

			if (left != join.Left || right != join.Right || condition != join.Condition)
			{
				return new JoinExpression(join.Join, left, right, condition);
			}
			return join;
		}
	}
}
