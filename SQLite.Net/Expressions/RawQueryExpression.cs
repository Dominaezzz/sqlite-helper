using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class RawQueryExpression : QueryExpression
    {
	    public RawQueryExpression(Type type, string alias, string sqlQuery) : base(alias)
	    {
		    Type = type;
		    SQLQuery = sqlQuery;
	    }

	    public override ExpressionType NodeType => (ExpressionType) DbExpressionType.RawQuery;
	    public override Type Type { get; }

	    /// <summary>
	    /// The name of the table.
	    /// </summary>
	    public string SQLQuery { get; }
	}
}
