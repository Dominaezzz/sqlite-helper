using SQLite.Net.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal class SubqueryRemover : DbExpressionVisitor
	{
		private readonly HashSet<SelectExpression> _selectsToRemove;
		private readonly Dictionary<string, Dictionary<string, Expression>> _map;

		private SubqueryRemover(IEnumerable<SelectExpression> selectsToRemove)
		{
			_selectsToRemove = new HashSet<SelectExpression>(selectsToRemove);
			_map = _selectsToRemove.ToDictionary(d => d.Alias, d => d.Columns.ToDictionary(d2 => d2.Name, d2 => d2.Expression));
		}

		internal static SelectExpression Remove(SelectExpression outerSelect, params SelectExpression[] selectsToRemove)
		{
			return Remove(outerSelect, (IEnumerable<SelectExpression>)selectsToRemove);
		}

		internal static SelectExpression Remove(SelectExpression outerSelect, IEnumerable<SelectExpression> selectsToRemove)
		{
			return (SelectExpression)new SubqueryRemover(selectsToRemove).Visit(outerSelect);
		}

		internal static ProjectionExpression Remove(ProjectionExpression projection, params SelectExpression[] selectsToRemove)
		{
			return Remove(projection, (IEnumerable<SelectExpression>)selectsToRemove);
		}

		internal static ProjectionExpression Remove(ProjectionExpression projection, IEnumerable<SelectExpression> selectsToRemove)
		{
			return (ProjectionExpression)new SubqueryRemover(selectsToRemove).Visit(projection);
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			return _selectsToRemove.Contains(select) ? Visit(select.From) : base.VisitSelect(select);
		}

		protected override Expression VisitColumn(ColumnExpression column)
		{
			if (_map.TryGetValue(column.Alias, out Dictionary<string, Expression> nameMap))
			{
				if (nameMap.TryGetValue(column.Name, out Expression expr))
				{
					return Visit(expr);
				}
				throw new Exception("Reference to undefined column");
			}
			return column;
		}
	}
}
