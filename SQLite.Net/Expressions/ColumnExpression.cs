using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal class ColumnExpression : DbExpression
	{
		internal ColumnExpression(Type type, string alias, string name)
		{
			Type = type;
			Alias = alias;
			Name = name;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Column;
		public override Type Type { get; }

		/// <summary>
		/// This is the alias of the source (table, subquery, etc.) this column comes from.
		/// </summary>
		internal string Alias { get; }
		/// <summary>
		/// This is the name of the column.
		/// </summary>
		internal string Name { get; }
	}
}
