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
	    private Expression _currentFrom = null;

		private AggregateSimplifier() { }

	    public static Expression Simplify(Expression expression)
	    {
		    return new AggregateSimplifier().Visit(expression);
	    }

	    protected override Expression VisitSelect(SelectExpression select)
	    {
		    var saveGroupBys = _currentGroupBys;
		    var saveFrom = _currentFrom;
			{
				if (select.GroupBy != null && select.GroupBy.Count > 0)
				{
					_currentGroupBys = select.GroupBy;
					_currentFrom = select.From;
				}
				else
				{
					_currentGroupBys = null;
					_currentFrom = null;
				}
				select = (SelectExpression)base.VisitSelect(select);
			}
		    _currentFrom = saveFrom;
		    _currentGroupBys = saveGroupBys;
		    return select;
	    }

	    protected override Expression VisitScalar(ScalarExpression scalar)
	    {
		    scalar = (ScalarExpression) base.VisitScalar(scalar);
		    if (_currentGroupBys != null && _currentFrom != null)
		    {
			    if (scalar.Query is SelectExpression innerSelect && SourcesAreEqual(_currentFrom, innerSelect.From))
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
					    else if (innerAggExpr.Argument is ColumnExpression column)
					    {
						    var source = GetSource(_currentFrom, innerSelect.From, column.Alias);
						    return new AggregateExpression(
							    innerAggExpr.Type,
							    innerAggExpr.AggregateType,
							    new ColumnExpression(column.Type, source.Alias, column.Name),
							    innerAggExpr.IsDistict
						    );
					    }
				    }
				}
			}
			return scalar;
	    }

	    private static bool SourcesAreEqual(Expression left, Expression right)
	    {
		    if (left == right) return true;
		    if (left == null || right == null) return false;
		    if (left.NodeType != right.NodeType) return false;

		    switch (left.NodeType)
		    {
				case (ExpressionType)DbExpressionType.Table:
					return ((TableExpression) left).Name == ((TableExpression) right).Name;
			    case (ExpressionType)DbExpressionType.View:
				    return ((ViewExpression) left).Name == ((ViewExpression) right).Name;
			    case (ExpressionType)DbExpressionType.Join:
				    JoinExpression leftJoin = (JoinExpression) left;
				    JoinExpression rightJoin = (JoinExpression) right;
				    return SourcesAreEqual(leftJoin.Left, rightJoin.Left) && SourcesAreEqual(leftJoin.Right, rightJoin.Right);
				default:
					return false;
			}
	    }

	    private static AliasedExpression GetSource(Expression main, Expression sub, string columnAlias)
	    {
			switch (sub)
			{
				case AliasedExpression aliasedExpression:
					if (aliasedExpression.Alias == columnAlias)
					{
						return (AliasedExpression) main;
					}
					return null;
				case JoinExpression subJoin:
					JoinExpression mainJoin = (JoinExpression)main;
					return GetSource(mainJoin.Left, subJoin.Left, columnAlias) ?? GetSource(mainJoin.Right, subJoin.Right, columnAlias);
				default:
					return null;
			}
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
