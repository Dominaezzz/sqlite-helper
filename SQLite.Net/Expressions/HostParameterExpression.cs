using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class HostParameterExpression : DbExpression
    {
	    public HostParameterExpression(Type type, object value)
	    {
		    Type = type;
		    Value = value;
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.HostParameter;
	    public override Type Type { get; }

		public object Value { get; set; }
    }
}
