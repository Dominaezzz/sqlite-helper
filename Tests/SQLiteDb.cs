using System;
using System.Collections.Generic;
using System.Text;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[Table("DataTable")]
	public class Data<T>
	{
		[PrimaryKey(AutoIncrement = true)]
		public int Id { get; set; }
		public T Value { get; set; }
	}

	public class SQLiteDb<T> : SQLiteDatabase
	{
		public Table<Data<T>> DataTable { get; set; }

		public SQLiteDb()
		{
			Log = Console.WriteLine;
			if (UserVersion == 0)
			{
				CreateTable("DataTable", c => new
				{
					Id = c.Column<int>(primaryKey: true, autoIncrement: true),
					Value = c.Column<T>()
				});
			}
			UserVersion++;
		}
	}
}
