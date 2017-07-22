using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Expressions;

namespace SQLite.Net
{
	/// <summary>
	/// A class representing View in an SQLite Database.
	/// </summary>
	/// <typeparam name="T">A model class to represent the view.</typeparam>
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
