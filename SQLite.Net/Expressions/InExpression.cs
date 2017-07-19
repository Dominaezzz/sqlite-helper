using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class InExpression : SubQueryExpression
    {
	    public InExpression(Expression expression, QueryExpression query) : base(typeof(bool), query)
	    {
		    Expression = expression;
	    }

	    public InExpression(Expression expression, IEnumerable<Expression> values) : base(typeof(bool), null)
	    {
		    Expression = expression;
		    Values = values.ToList().AsReadOnly();
	    }

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.In;

	    public Expression Expression { get; }
		public ReadOnlyCollection<Expression> Values { get; }
    }
}
