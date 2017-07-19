using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	/// <summary>
	///  returns the set of all aliases produced by a query source
	/// </summary>
	internal class AliasesProduced : DbExpressionVisitor
	{
		private readonly HashSet<string> _aliases = new HashSet<string>();

		private AliasesProduced() { }

		public static HashSet<string> Gather(Expression source)
		{
			AliasesProduced ap = new AliasesProduced();
			ap.Visit(source);
			return ap._aliases;
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			_aliases.Add(select.Alias);
			return select;
		}

		protected override Expression VisitTable(TableExpression table)
		{
			_aliases.Add(table.Alias);
			return table;
		}
	}
}
