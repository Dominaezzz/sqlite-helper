using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
    internal class AggregateSimplifier : DbExpressionVisitor
    {
	    private ReadOnlyCollection<Expression> _currentGroupBys = null;
	    private List<TableExpression> _currentTables = null;

		private AggregateSimplifier() { }

	    public static Expression Simplify(Expression expression)
	    {
		    return new AggregateSimplifier().Visit(expression);
	    }

	    protected override Expression VisitSelect(SelectExpression select)
	    {
		    var saveGroupBys = _currentGroupBys;
		    var saveTables = _currentTables;
			{
				if (select.GroupBy != null && select.GroupBy.Count > 0)
				{
					_currentGroupBys = select.GroupBy;
				}
				else
				{
					_currentGroupBys = null;
				}
				_currentTables = null;
				select = (SelectExpression)base.VisitSelect(select);
			}
		    _currentTables = saveTables;
		    _currentGroupBys = saveGroupBys;
		    return select;
	    }

	    protected override Expression VisitTable(TableExpression table)
	    {
		    if (_currentGroupBys != null)
		    {
			    if (_currentTables == null) _currentTables = new List<TableExpression>();
				_currentTables.Add(table);
		    }
		    return base.VisitTable(table);
	    }

	    protected override Expression VisitScalar(ScalarExpression scalar)
	    {
		    scalar = (ScalarExpression) base.VisitScalar(scalar);
		    if (_currentGroupBys != null && _currentTables != null)
		    {
			    if (scalar.Query is SelectExpression innerSelect && innerSelect.From is TableExpression innerTable)
			    {
				    TableExpression table = _currentTables.Find(t => t.Name == innerTable.Name);
				    if (table != null)
				    {
					    var groupedColumns = GroupedColumnGatherer.Gather(innerSelect.Where);
					    if (groupedColumns?.SequenceEqual(_currentGroupBys) ?? false)
					    {
						    var innerAggExpr = (AggregateExpression)innerSelect.Columns[0].Expression;
						    if (innerAggExpr.Argument == null)
						    {
								return new AggregateExpression(
									innerAggExpr.Type,
									innerAggExpr.AggregateType,
									null,
									innerAggExpr.IsDistict
								);
							}
						    else if(innerAggExpr.Argument is ColumnExpression column)
						    {
								return new AggregateExpression(
									innerAggExpr.Type,
									innerAggExpr.AggregateType,
									new ColumnExpression(column.Type, table.Alias, column.Name),
									innerAggExpr.IsDistict
								);
							}
					    }
				    }
			    }
			}
			return scalar;
	    }

	    private class GroupedColumnGatherer : DbExpressionVisitor
	    {
			private List<ColumnExpression> _columns;
		    private bool _failed = false;

		    private GroupedColumnGatherer() { }

		    public static List<ColumnExpression> Gather(Expression expression)
		    {
				var g = new GroupedColumnGatherer();
			    g.Visit(expression);
			    return g._columns;
		    }
			
			public override Expression Visit(Expression exp)
			{
				switch (exp.NodeType)
				{
					case ExpressionType.AndAlso:
						return base.Visit(exp);
					case ExpressionType.Equal:
						BinaryExpression bin = (BinaryExpression) exp;
						if (!_failed && bin.Left is ColumnExpression left && bin.Right is ColumnExpression right)
						{
							if (left.Name == right.Name)
							{
								if(_columns == null) _columns = new List<ColumnExpression>();
								_columns.Add(right);
								return exp;
							}
						}
						_failed = true;
						return exp;
					default:
						_failed = true;
						return exp;
				}
			}
	    }
    }
}
