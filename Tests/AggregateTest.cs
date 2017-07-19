using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[TestFixture(Category = "Aggregate")]
    public class AggregateTest
	{
		private const int RowCount = 100;

		private SQLiteDb<string> _db;

		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new SQLiteDb<string>();
			_db.DataTable.Insert(Enumerable.Range(0, RowCount).Select(i => new Data<string> { Value = $"Some Text {i}" }));
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}


		[Test(ExpectedResult = 1)]
		public int TestMin()
		{
			return _db.DataTable.Min(d => d.Id);
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25, ExpectedResult = 5)]
		[TestCase(45, 65, ExpectedResult = 45)]
		[TestCase(37, 40, ExpectedResult = 37)]
		public int TestMinWhere(int min, int max)
		{
			return _db.DataTable
				.Where(d => d.Id >= min && d.Id <= max)
				.Min(d => d.Id);
		}


		[Test(ExpectedResult = 100)]
		public int TestMax()
		{
			return _db.DataTable.Max(d => d.Id);
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25, ExpectedResult = 25)]
		[TestCase(45, 65, ExpectedResult = 65)]
		[TestCase(37, 40, ExpectedResult = 40)]
		public int TestMaxWhere(int min, int max)
		{
			return _db.DataTable
				.Where(d => d.Id >= min && d.Id <= max)
				.Max(d => d.Id);
		}


		[Test(ExpectedResult = (100 + 1) * 100 / 2)]
		public int TestSum()
		{
			return _db.DataTable.Sum(d => d.Id);
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25, ExpectedResult = 315)]
		[TestCase(45, 65, ExpectedResult = 1155)]
		[TestCase(37, 40, ExpectedResult = 154)]
		public int TestSumWhere(int min, int max)
		{
			return _db.DataTable
				.Where(d => d.Id >= min && d.Id <= max)
				.Sum(d => d.Id);
		}


		[Test(ExpectedResult = (100 + 1) / 2.0)]
		public double TestAverage()
		{
			return _db.DataTable.Average(d => d.Id);
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25, ExpectedResult = 15.0)]
		[TestCase(45, 65, ExpectedResult = 55.0)]
		[TestCase(37, 40, ExpectedResult = 38.5)]
		public double TestAverageWhere(int min, int max)
		{
			return _db.DataTable
				.Where(d => d.Id >= min && d.Id <= max)
				.Average(d => d.Id);
		}


		[Test(ExpectedResult = 100)]
		public int TestCount()
		{
			return _db.DataTable.Count();
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25, ExpectedResult = 21)]
		[TestCase(45, 75, ExpectedResult = 31)]
		[TestCase(37, 40, ExpectedResult = 4)]
		public int TestCountWhere(int min, int max)
		{
			return _db.DataTable.Count(d => d.Id >= min && d.Id <= max);
		}
	}
}
