using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[TestFixture(Category = "Where")]
    public class WhereTest
    {
		private const int RowCount = 100;

	    private SQLiteDb _db;

		[OneTimeSetUp]
	    public void TestSetUp()
	    {
		    _db = new SQLiteDb();
		    _db.DataTable.Insert(Enumerable.Range(0, RowCount).Select(i => new Data { TextValue = $"Some Text {i}" }));
	    }

		[OneTimeTearDown]
	    public void TestTearDown()
	    {
		    _db.Dispose();
	    }


		[Test]
		[TestCase(10, ExpectedResult = 1)]
		[TestCase(50, ExpectedResult = 1)]
		[TestCase(90, ExpectedResult = 1)]
		public int TestEqual(int value)
		{
			return _db.DataTable
				.Where(d => d.Id == value)
				.ToList()
				.Count;
		}

		[Test]
		[TestCase(10, ExpectedResult = 99)]
		[TestCase(50, ExpectedResult = 99)]
		[TestCase(90, ExpectedResult = 99)]
		public int TestNotEqual(int value)
		{
			return _db.DataTable
				.Where(d => d.Id != value)
				.ToList()
				.Count;
		}

	    [Test]
	    [TestCase(10, ExpectedResult = 91)]
	    [TestCase(50, ExpectedResult = 51)]
	    [TestCase(90, ExpectedResult = 11)]
		public int TestGreaterThanOrEqualTo(int value)
	    {
		    return _db.DataTable
			    .Where(d => d.Id >= value)
			    .ToList()
			    .Count;
		}

	    [Test]
	    [TestCase(10, ExpectedResult = 90)]
	    [TestCase(50, ExpectedResult = 50)]
	    [TestCase(90, ExpectedResult = 10)]
		public int TestGreaterThan(int value)
	    {
		    return _db.DataTable
			    .Where(d => d.Id > value)
			    .ToList()
			    .Count;
		}

	    [Test]
	    [TestCase(10, ExpectedResult = 10)]
	    [TestCase(50, ExpectedResult = 50)]
	    [TestCase(90, ExpectedResult = 90)]
		public int TestLessThanOrEqualTo(int value)
	    {
		    return _db.DataTable
			    .Where(d => d.Id <= value)
			    .ToList()
			    .Count;
		}

	    [Test]
	    [TestCase(10, ExpectedResult =  9)]
	    [TestCase(50, ExpectedResult = 49)]
	    [TestCase(90, ExpectedResult = 89)]
		public int TestLessThan(int value)
	    {
		    return _db.DataTable
			    .Where(d => d.Id < value)
			    .ToList()
			    .Count;
	    }

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
