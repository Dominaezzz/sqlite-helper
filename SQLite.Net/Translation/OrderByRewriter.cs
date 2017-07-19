using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	/// <summary>
	/// Move order-bys to the outermost select
	/// </summary>
	internal class OrderByRewriter : DbExpressionVisitor
	{
		private IEnumerable<OrderExpression> _gatheredOrderings;
		private bool _isOuterMostSelect = true;

		private OrderByRewriter() { }

		public static Expression Rewrite(Expression expression)
		{
			return new OrderByRewriter().Visit(expression);
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			bool saveIsOuterMostSelect = _isOuterMostSelect;
			try
			{
				_isOuterMostSelect = false;

				select = (SelectExpression)base.VisitSelect(select);

				bool hasOrderBy = select.OrderBy != null && select.OrderBy.Count > 0;
				if (hasOrderBy) PrependOrderings(select.OrderBy);

				bool canHaveOrderBy = saveIsOuterMostSelect;
				bool canPassOnOrderings = !saveIsOuterMostSelect;

				IEnumerable<OrderExpression> orderings = (canHaveOrderBy) ? _gatheredOrderings : null;

				ReadOnlyCollection<ColumnDeclaration> columns = select.Columns;

				if (_gatheredOrderings != null)
				{
					if (canPassOnOrderings)
					{
						HashSet<string> producedAliases = AliasesProduced.Gather(select.From);

						// reproject order expressions using this select’s alias so the outer select will have properly formed expressions
						BindResult project = RebindOrderings(_gatheredOrderings, select.Alias, producedAliases, select.Columns);
						_gatheredOrderings = project.Orderings;
						columns = project.Columns;
					}
					else
					{
						_gatheredOrderings = null;
					}
				}

				if (orderings != select.OrderBy || columns != select.Columns)
				{
					select = new SelectExpression(select.Alias, columns, select.From, select.Where, select.GroupBy, select.Having, orderings, select.Offset, select.Limit, select.IsDistinct);
				}
				return select;
			}
			finally
			{
				_isOuterMostSelect = saveIsOuterMostSelect;
			}
		}

		protected override Expression VisitSubquery(SubQueryExpression subquery)
		{
			var saveOrderings = _gatheredOrderings;
			_gatheredOrderings = null;
			var result = base.VisitSubquery(subquery);
			_gatheredOrderings = saveOrderings;
			return result;
		}

		protected override Expression VisitJoin(JoinExpression join)
		{
			// make sure order by expressions lifted up from the left side are not lost
			// when visiting the right side
			Expression left = VisitSource(join.Left);
			IEnumerable<OrderExpression> leftOrders = _gatheredOrderings;

			_gatheredOrderings = null; // start on the right with a clean slate

			Expression right = VisitSource(join.Right);

			PrependOrderings(leftOrders);

			Expression condition = Visit(join.Condition);

			if (left != join.Left || right != join.Right || condition != join.Condition)
			{
				return new JoinExpression(join.Join, left, right, condition);
			}
			return join;
		}

		/// <summary>
		/// Add a sequence of order expressions to an accumulated list, prepending so as
		/// to give precedence to the new expressions over any previous expressions
		/// </summary>
		/// <param name="newOrderings"></param>
		protected void PrependOrderings(IEnumerable<OrderExpression> newOrderings)
		{
			if (newOrderings == null) return;

			if (_gatheredOrderings == null)
			{
				_gatheredOrderings = newOrderings;
			}
			else
			{
				List<OrderExpression> list = _gatheredOrderings as List<OrderExpression>;
				if (list == null)
				{
					_gatheredOrderings = list = new List<OrderExpression>(_gatheredOrderings);
				}
				list.InsertRange(0, newOrderings);
			}
		}

		protected class BindResult
		{
			public BindResult(IEnumerable<ColumnDeclaration> columns, IEnumerable<OrderExpression> orderings)
			{
				Columns = columns as ReadOnlyCollection<ColumnDeclaration> ?? columns.ToList().AsReadOnly();
				Orderings = orderings as ReadOnlyCollection<OrderExpression> ?? orderings.ToList().AsReadOnly();
			}

			public ReadOnlyCollection<ColumnDeclaration> Columns { get; }
			public ReadOnlyCollection<OrderExpression> Orderings { get; }
		}

		/// <summary>
		/// Rebind order expressions to reference a new alias and add to column declarations if necessary
		/// </summary>
		protected virtual BindResult RebindOrderings(IEnumerable<OrderExpression> orderings, string alias, HashSet<string> existingAliases, IEnumerable<ColumnDeclaration> existingColumns)
		{
			List<ColumnDeclaration> newColumns = null;
			List<OrderExpression> newOrderings = new List<OrderExpression>();

			var columnDeclarations = existingColumns as IList<ColumnDeclaration> ?? existingColumns.ToList();

			foreach (OrderExpression ordering in orderings)
			{
				Expression expr = ordering.Expression;
				ColumnExpression column = expr as ColumnExpression;
				if (column != null && (existingAliases == null || !existingAliases.Contains(column.Alias))) continue;

				// check to see if a declared column already contains a similar expression
				int iOrdinal = 0;

				foreach (ColumnDeclaration decl in columnDeclarations)
				{
					ColumnExpression declColumn = decl.Expression as ColumnExpression;
					if (decl.Expression == ordering.Expression ||
					    (column != null && declColumn != null && column.Alias == declColumn.Alias && column.Name == declColumn.Name))
					{
						// found it, so make a reference to this column
						expr = new ColumnExpression(column.Type, alias, decl.Name);
						break;
					}
					iOrdinal++;
				}

				// if not already projected, add a new column declaration for it
				if (expr == ordering.Expression)
				{
					if (newColumns == null)
					{
						newColumns = columnDeclarations.ToList();
						existingColumns = newColumns;
					}

					string colName = column?.Name ?? "c" + iOrdinal;
					newColumns.Add(new ColumnDeclaration(colName, ordering.Expression));
					expr = new ColumnExpression(expr.Type, alias, colName);
				}
				newOrderings.Add(new OrderExpression(ordering.OrderType, expr));
			}
			return new BindResult(columnDeclarations, newOrderings);
		}
	}
}
