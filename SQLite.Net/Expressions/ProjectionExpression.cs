using System;
using System.Linq;
using System.Linq.Expressions;

namespace SQLite.Net.Expressions
{
	/// <summary>
	/// A custom expression representing the construction of one or more result objects from a 
	/// SQL select expression
	/// </summary>
	internal class ProjectionExpression : DbExpression
	{
		internal ProjectionExpression(QueryExpression source, Expression projector, LambdaExpression aggregator = null)
		{
			Type = aggregator?.ReturnType ?? typeof(IQueryable<>).MakeGenericType(projector.Type);
			Source = source;
			Projector = projector;
			Aggregator = aggregator;
		}

		public override ExpressionType NodeType => (ExpressionType) DbExpressionType.Projection;
		public override Type Type { get; }
		
        /// <summary>
        /// Query statement with columns to be projected into objects.
        /// </summary>
		public QueryExpression Source { get; }

        /// <summary>
        /// Expression to take data from columns and return an object for each row.
        /// Usually a MemberInitExpression or NewExpression.
        /// </summary>
		public Expression Projector { get; }

        /// <summary>
        /// If the select expression returns a single result.
        /// This takes the resulting enumerable and return an object.
        /// </summary>
		public LambdaExpression Aggregator { get; }
	}
}
