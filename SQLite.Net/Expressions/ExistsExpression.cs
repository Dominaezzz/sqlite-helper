using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class ExistsExpression : SubQueryExpression
    {
	    public ExistsExpression(QueryExpression query) : base(typeof(bool), query)
	    {
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Exists;
    }
}
