using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal class RedundantSubqueryRemover : DbExpressionVisitor
	{
		private RedundantSubqueryRemover(){}

		internal static Expression Remove(Expression expression)
		{
			expression = new RedundantSubqueryRemover().Visit(expression);
			expression = SubqueryMerger.Merge(expression);
			return expression;
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			select = (SelectExpression)base.VisitSelect(select);

			// first remove all purely redundant subqueries
			List<SelectExpression> redundant = RedundantSubqueryGatherer.Gather(select.From);
			if (redundant != null)
			{
				select = SubqueryRemover.Remove(select, redundant);
			}
			
			return select;
		}
		
		protected override Expression VisitProjection(ProjectionExpression proj)
		{
			proj = (ProjectionExpression)base.VisitProjection(proj);
			if (proj.Source is SelectExpression select && select.From is SelectExpression)
			{
				List<SelectExpression> redundant = RedundantSubqueryGatherer.Gather(proj.Source);
				if (redundant != null)
				{
					proj = SubqueryRemover.Remove(proj, redundant);
				}
			}
			return proj;
		}

		private static bool ProjectionIsSimple(SelectExpression select)
		{
			return select.Columns.All(c => c.Expression is ColumnExpression && ((ColumnExpression)c.Expression).Name == c.Name);
		}

		private static bool IsNameMapProjection(SelectExpression select)
		{
			if (select.From is SelectExpression fromSelect && select.Columns.Count == fromSelect.Columns.Count)
			{
				// test that all columns in 'select' are refering to columns in the same position
				// in 'fromSelect'.
				return select.Columns.Zip(
					fromSelect.Columns,
					(col, fCol) => col.Expression is ColumnExpression && ((ColumnExpression)col.Expression).Name == fCol.Name
				)
				.All(equal => equal);
			}
			return false;
		}

		private class RedundantSubqueryGatherer : DbExpressionVisitor
		{
			private List<SelectExpression> _redundant;

			private RedundantSubqueryGatherer() { }

			internal static List<SelectExpression> Gather(Expression source)
			{
				RedundantSubqueryGatherer gatherer = new RedundantSubqueryGatherer();
				gatherer.Visit(source);
				return gatherer._redundant;
			}

			private static bool IsRedudantSubquery(SelectExpression select)
			{
				return select.From is AliasedExpression
				       && (ProjectionIsSimple(select) || IsNameMapProjection(select))
					   && !select.IsDistinct
				       && select.Where == null
					   && select.Limit == null
					   && select.Offset == null
					   && select.Having == null
				       && (select.GroupBy == null || select.GroupBy.Count == 0)
				       && (select.OrderBy == null || select.OrderBy.Count == 0);
			}

			protected override Expression VisitSelect(SelectExpression select)
			{
				if (IsRedudantSubquery(select))
				{
					if (_redundant == null)
					{
						_redundant = new List<SelectExpression>();
					}
					_redundant.Add(select);
				}
				return select;
			}

			protected override Expression VisitSubquery(SubQueryExpression subquery)
			{
				// don't gather inside scalar & exists
				return subquery;
			}
		}

		private class SubqueryMerger : DbExpressionVisitor
		{
			private bool _isTopLevel = true;

			private SubqueryMerger(){}

			internal static Expression Merge(Expression expression)
			{
				return new SubqueryMerger().Visit(expression);
			}

			protected override Expression VisitSelect(SelectExpression select)
			{
				bool wasTopLevel = _isTopLevel;
				_isTopLevel = false;

				select = (SelectExpression)base.VisitSelect(select);

				// next attempt to merge subqueries that would have been removed by the above
				// logic except for the existence of a where clause
				while (CanMergeWithFrom(select, wasTopLevel))
				{
					SelectExpression fromSelect = GetLeftMostSelect(select.From);

					// remove the redundant subquery
					select = SubqueryRemover.Remove(select, fromSelect);

					// merge where expressions 
					Expression where;
					Expression having;
					if (fromSelect.GroupBy != null && fromSelect.GroupBy.Count > 0)
					{
						where = fromSelect.Where;
						having = select.Where;
						if (fromSelect.Having != null)
						{
							having = having != null ? Expression.AndAlso(fromSelect.Having, having) : fromSelect.Having;
						}
					}
					else
					{
						where = select.Where;
						if (fromSelect.Where != null)
						{
							where = where != null ? Expression.AndAlso(fromSelect.Where, where) : fromSelect.Where;
						}
						having = null;
					}
					
					var orderBy = select.OrderBy != null && select.OrderBy.Count > 0 ? select.OrderBy : fromSelect.OrderBy;
					var groupBy = select.GroupBy != null && select.GroupBy.Count > 0 ? select.GroupBy : fromSelect.GroupBy;
					Expression offset = select.Offset;
					if (fromSelect.Offset != null)
					{
						offset = offset == null ? fromSelect.Offset : Expression.Add(fromSelect.Offset, offset);
					}
					Expression limit = select.Limit ?? fromSelect.Limit;
					if (fromSelect.Limit != null)
					{
						limit = limit == null ? fromSelect.Limit :
							Expression.Call(typeof(Math), nameof(Math.Min), null, fromSelect.Limit, limit);
					}
					bool isDistinct = select.IsDistinct | fromSelect.IsDistinct;

					if (where != select.Where
					    || orderBy != select.OrderBy
					    || groupBy != select.GroupBy
						|| having != select.Having
					    || isDistinct != select.IsDistinct
					    || offset != select.Offset
					    || limit != select.Limit)
					{
						select = new SelectExpression(
							select.Alias, select.Columns, select.From, where, groupBy, having, orderBy, offset, limit, isDistinct
						);
					}
				}

				return select;
			}

			private static SelectExpression GetLeftMostSelect(Expression source)
			{
				if (source is SelectExpression select) return select;
				if (source is JoinExpression join) return GetLeftMostSelect(join.Left);
				return null;
			}

			private static bool IsColumnProjection(SelectExpression select)
			{
				return !select.Columns.Any(cd => cd.Expression.NodeType != (ExpressionType) DbExpressionType.Column &&
				                                 cd.Expression.NodeType != ExpressionType.Constant);
			}

			private static bool CanMergeWithFrom(SelectExpression select, bool isTopLevel)
			{
				SelectExpression fromSelect = GetLeftMostSelect(select.From);
				if (fromSelect == null) return false;
				//if (!IsColumnProjection(fromSelect)) return false;

				bool selHasNameMapProjection = IsNameMapProjection(select);
				bool selHasOrderBy = select.OrderBy != null && select.OrderBy.Count > 0;
				bool selHasGroupBy = select.GroupBy != null && select.GroupBy.Count > 0;
				bool selHasAggregates = AggregateChecker.HasAggregates(select);
				bool selHasJoin = select.From is JoinExpression;
				bool frmHasOrderBy = fromSelect.OrderBy != null && fromSelect.OrderBy.Count > 0;
				bool frmHasGroupBy = fromSelect.GroupBy != null && fromSelect.GroupBy.Count > 0;
				bool frmHasAggregates = AggregateChecker.HasAggregates(fromSelect);

				// both cannot have orderby
				if (selHasOrderBy && frmHasOrderBy) return false;
				// both cannot have groupby
				if (selHasGroupBy && frmHasGroupBy) return false;
				// cannot move forward order-by if outer has group-by
				if (frmHasOrderBy && (selHasGroupBy || selHasAggregates || select.IsDistinct)) return false;
				// cannot move forward a limit or an offset if outer has distinct
				if (fromSelect.Limit != null && fromSelect.Offset != null && (select.IsDistinct || selHasAggregates || selHasGroupBy || selHasJoin)) return false;
				// cannot move forward a distinct if outer has groupby or a different projection
				if (fromSelect.IsDistinct && (!selHasNameMapProjection || selHasGroupBy || selHasAggregates || (selHasOrderBy && !isTopLevel) || selHasJoin))
					return false;
				if (frmHasAggregates && (select.IsDistinct || selHasAggregates || selHasGroupBy || selHasJoin)) return false;

				return true;
			}
		}
	}
}
