using System;
using System.Linq;
using SQLite.Net;
using SQLite.Net.Attributes;
using Xunit;

namespace Tests
{
	[Trait("Category", "Where")]
    public class WhereTest : IDisposable
    {
		private const int RowCount = 100;

	    private readonly SQLiteDb _db;

	    public WhereTest()
	    {
		    _db = new SQLiteDb();
		    _db.DataTable.Insert(Enumerable.Range(0, RowCount).Select(i => new Data { TextValue = $"Some Text {i}" }));
	    }

	    public void Dispose()
	    {
		    _db.Dispose();
	    }


		[Theory]
		[InlineData(10)]
		[InlineData(50)]
		[InlineData(90)]
		public void TestEqual(int value)
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id == ?", value);
			int actual = _db.DataTable.Count(d => d.Id == value);
			Assert.Equal(expected, actual);
		}

		[Theory]
		[InlineData(10)]
		[InlineData(50)]
		[InlineData(90)]
		public void TestNotEqual(int value)
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id != ?", value);
			int actual = _db.DataTable.Count(d => d.Id != value);
			Assert.Equal(expected, actual);
		}

	    [Theory]
	    [InlineData(10)]
	    [InlineData(50)]
	    [InlineData(90)]
		public void TestGreaterThanOrEqualTo(int value)
	    {
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id >= ?", value);
			int actual = _db.DataTable.Count(d => d.Id >= value);
			Assert.Equal(expected, actual);
		}

	    [Theory]
	    [InlineData(10)]
	    [InlineData(50)]
	    [InlineData(90)]
		public void TestGreaterThan(int value)
	    {
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id > ?", value);
			int actual = _db.DataTable.Count(d => d.Id > value);
			Assert.Equal(expected, actual);
		}

	    [Theory]
	    [InlineData(10)]
	    [InlineData(50)]
	    [InlineData(90)]
		public void TestLessThanOrEqualTo(int value)
	    {
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id <= ?", value);
			int actual = _db.DataTable.Count(d => d.Id <= value);
			
			Assert.Equal(expected, actual);
		}

	    [Theory]
	    [InlineData(10)]
	    [InlineData(50)]
	    [InlineData(90)]
		public void TestLessThan(int value)
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable WHERE Id < ?", value);
			int actual = _db.DataTable.Count(d => d.Id < value);
			
			Assert.Equal(expected, actual);
	    }

		[Table("DataTable")]
		public class Data
		{
			[PrimaryKey(AutoIncrement = true)]
			public int Id { get; set; }
			public string TextValue { get; set; }
		}

		private class SQLiteDb : SQLiteDatabase
		{
			public Table<Data> DataTable { get; set; }

			public SQLiteDb()
			{
				Log = Console.WriteLine;
				if (UserVersion == 0)
				{
					CreateTable("DataTable", c => new
					{
						Id = c.Column<int>(primaryKey: true, autoIncrement: true),
						TextValue = c.Column<string>()
					});
				}
				UserVersion++;
			}
		}
	}
}
