using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class FunctionExpression : DbExpression
	{
		public FunctionExpression(Type type, string name, params Expression[] arguments)
		{
			Type = type;
			Name = name;
			Arguments = arguments.ToList().AsReadOnly();
		}

		public FunctionExpression(Type type, string name, IEnumerable<Expression> arguments)
	    {
		    Type = type;
		    Name = name;
		    Arguments = arguments as ReadOnlyCollection<Expression> ?? arguments.ToList().AsReadOnly();
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Function;
	    public override Type Type { get; }

		public string Name { get; }
		public ReadOnlyCollection<Expression> Arguments { get; }
    }
}
