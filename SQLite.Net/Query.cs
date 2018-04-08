using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SQLite.Net.Expressions;

namespace SQLite.Net
{
	public class Query<T> : IOrderedQueryable<T>
	{
		internal Query(SQLiteQueryProvider provider)
		{
			Provider = provider;
			Expression = Expression.Constant(this);
		}

		internal Query(SQLiteQueryProvider provider, Expression expression)
		{
			if(expression == null) throw new ArgumentNullException(nameof(expression));

			if (!typeof(IQueryable<T>).GetTypeInfo().IsAssignableFrom(expression.Type.GetTypeInfo()))
			{
				throw new ArgumentOutOfRangeException(nameof(expression));
			}
			Expression = expression;
			Provider = provider;
		}

		public IEnumerator<T> GetEnumerator()
		{
			return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public Type ElementType => typeof(T);
		public Expression Expression { get; }
		public IQueryProvider Provider { get; }
	}

	internal class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
	{
		private readonly IEnumerable<TElement> _group;
		
		public Grouping(TKey key, IEnumerable<TElement> group)
		{
			Key = key;
			_group = group;
		}
		
		public TKey Key { get; }
		
		public IEnumerator<TElement> GetEnumerator()
		{
			return _group.GetEnumerator();
		}
		
		IEnumerator IEnumerable.GetEnumerator()
		{
			return _group.GetEnumerator();
		}
	}
}
