using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;

namespace SQLite.Net
{
    public class View<T> : Query<T>
	{
		public string Name { get; }

		internal View(SQLiteQueryProvider provider, string name) : base(provider)
		{
			Name = name;
		}

		public override string ToString()
		{
			return Name;
		}
	}
}
