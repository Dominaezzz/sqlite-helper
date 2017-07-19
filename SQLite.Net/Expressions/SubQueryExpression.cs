using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal abstract class SubQueryExpression : DbExpression
	{
		protected SubQueryExpression(Type type, QueryExpression query)
		{
			Type = type;
			Query = query;
		}
		
		public override Type Type { get; }

        /// <summary>
        /// The query statement of this subquery.
        /// </summary>
		public QueryExpression Query { get; }
	}
}
