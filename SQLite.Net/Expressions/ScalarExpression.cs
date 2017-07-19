using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class ScalarExpression : SubQueryExpression
    {
	    public ScalarExpression(Type type, QueryExpression query) : base(type, query)
	    {
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Scalar;
    }
}
