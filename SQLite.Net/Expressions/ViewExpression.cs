using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class ViewExpression : AliasedExpression
    {
	    public ViewExpression(string alias, string name) : base(alias)
	    {
		    Name = name;
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.View;
	    public override Type Type => typeof(void);

	    /// <summary>
	    /// The name of the table.
	    /// </summary>
	    public string Name { get; }
	}
}
