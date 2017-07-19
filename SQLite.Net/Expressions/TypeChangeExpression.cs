using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class TypeChangeExpression : DbExpression
    {
	    public TypeChangeExpression(Type type, Expression expression)
	    {
		    Type = type;
		    Expression = expression;
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.TypeChange;
	    public override Type Type { get; }

		public Expression Expression { get; }
    }
}
