using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal class TableExpression : AliasedExpression
	{
		public TableExpression(string alias, string name) : base(alias)
		{
			Name = name;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Table;
		public override Type Type => typeof(void);

        /// <summary>
        /// The name of the table.
        /// </summary>
		public string Name { get; }
	}
}
