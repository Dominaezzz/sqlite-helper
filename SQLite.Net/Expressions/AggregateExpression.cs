using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLite.Net.Expressions
{
	internal enum AggregateType
	{
		Count,
		Min,
		Max,
		Sum,
		Average
	}
	
	internal class AggregateExpression : DbExpression
	{
		internal AggregateExpression(Type type, AggregateType aggType, Expression argument, bool isDistict = false)
		{
			Type = type;
			AggregateType = aggType;
			Argument = argument;
		    IsDistict = isDistict;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Aggregate;
		public override Type Type { get; }

        /// <summary>
        /// The name of aggregate function this describes.
        /// </summary>
		internal AggregateType AggregateType { get; }
        /// <summary>
        /// Argument for the aggragate.
        /// </summary>
		internal Expression Argument { get; }
        internal bool IsDistict { get; }
	}
}
