using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net.Expressions
{
    internal abstract class AliasedExpression : DbExpression
    {
	    protected AliasedExpression(string alias)
	    {
		    Alias = alias;
	    }

		/// <summary>
		/// This is an alias attached to this source when applicable. To be used by an other query.
		/// </summary>
		public string Alias { get; }
	}
}
