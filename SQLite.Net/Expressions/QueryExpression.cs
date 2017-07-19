using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal class QueryExpression : AliasedExpression
    {
	    public QueryExpression(string alias) : base(alias)
	    {
	    }
    }
}
