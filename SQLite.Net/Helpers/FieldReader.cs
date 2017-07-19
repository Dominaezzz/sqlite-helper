using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;

namespace SQLite.Net.Helpers
{
    public interface IFieldReader
    {
	    T Get<T>(string columnName);
    }

	internal class FieldReaderReplacer : DbExpressionVisitor
	{
		private readonly string _alias;

		private FieldReaderReplacer(string alias)
		{
			_alias = alias;
		}

		public static Expression Replace(string alias, Expression expression)
		{
			return new FieldReaderReplacer(alias).Visit(expression);
		}

		protected override Expression VisitMethodCall(MethodCallExpression node)
		{
			if (node.Method.DeclaringType == typeof(IFieldReader))
			{
				string columnName = (string)Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke();
				return new ColumnExpression(node.Method.ReturnType, _alias, columnName);
			}
			return base.VisitMethodCall(node);
		}
	}
}
