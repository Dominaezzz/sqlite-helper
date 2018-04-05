using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;

namespace SQLite.Net.Helpers
{
    internal class ConstantEscaper : DbExpressionVisitor
    {
		private ConstantEscaper() { }

	    public static Expression EscapeConstants(Expression expression)
	    {
		    return new ConstantEscaper().Visit(expression);
	    }

	    protected override Expression VisitConstant(ConstantExpression node)
	    {
		    if (Orm.IsColumnTypeSupported(node.Type))
		    {
			    return new HostParameterExpression(node.Type, node.Value);
		    }
		    return base.VisitConstant(node);
	    }

		protected override Expression VisitProjection(ProjectionExpression proj)
		{
			QueryExpression source = (QueryExpression)Visit(proj.Source);

			if (source != proj.Source)
			{
				return new ProjectionExpression(source, proj.Projector, proj.Aggregator);
			}
			return proj;
		}
	}
}
