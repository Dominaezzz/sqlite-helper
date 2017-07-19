using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal enum JoinType
	{
		CrossJoin,
		InnerJoin,
		OuterJoin,
	}

	internal class JoinExpression : DbExpression
	{
		internal JoinExpression(JoinType joinType, Expression left, Expression right, Expression condition)
		{
			Join = joinType;
			Left = left;
			Right = right;
			Condition = condition;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Join;
		public override Type Type => typeof(void);

		internal JoinType Join { get; }
		internal Expression Left { get; }
		internal Expression Right { get; }
		internal new Expression Condition { get; }
	}
}
