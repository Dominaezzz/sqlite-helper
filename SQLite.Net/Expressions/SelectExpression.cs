using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal enum OrderType
	{
		Ascending,
		Descending
	}

	internal class OrderExpression
	{
		internal OrderExpression(OrderType orderType, Expression expression)
		{
			OrderType = orderType;
			Expression = expression;
		}

		internal OrderType OrderType { get; }
		internal Expression Expression { get; }
	}

	internal class ColumnDeclaration
	{
		internal ColumnDeclaration(string name, Expression expression)
		{
			Name = name;
			Expression = expression;
		}

		/// <summary>
		/// This is the name of this result-column.
		/// </summary>
		internal string Name { get; }
		/// <summary>
		/// This is the expression of this result-column.
		/// </summary>
		internal Expression Expression { get; }
	}

	internal class SelectExpression : QueryExpression
	{
		internal SelectExpression(string alias, IEnumerable<ColumnDeclaration> columns,
			Expression from, Expression where = null,
            IEnumerable<Expression> groupBy = null, Expression having = null,
			IEnumerable<OrderExpression> orderBy = null, Expression offset = null, Expression limit = null,
			bool isDistinct = false) : base(alias)
		{
			Columns = columns as ReadOnlyCollection<ColumnDeclaration> ?? columns.ToList().AsReadOnly();
			From = from;
			IsDistinct = isDistinct;
			Where = where;
			GroupBy = groupBy as ReadOnlyCollection<Expression> ?? groupBy?.ToList().AsReadOnly();
		    Having = having;
			OrderBy = orderBy as ReadOnlyCollection<OrderExpression> ?? orderBy?.ToList().AsReadOnly();
			Offset = offset;
			Limit = limit;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Select;
		public override Type Type => typeof(void);

		public bool IsDistinct { get; }
		public ReadOnlyCollection<ColumnDeclaration> Columns { get; }
		public Expression From { get; }
		public Expression Where { get; }
		public ReadOnlyCollection<Expression> GroupBy { get; }
        public Expression Having { get; }
		public ReadOnlyCollection<OrderExpression> OrderBy { get; }
		public Expression Offset { get; }
		public Expression Limit { get; }
	}
}
