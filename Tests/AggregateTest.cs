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


		[Test]
		public void TestMin()
		{
			int expected = _db.ExecuteScalar<int>("SELECT MIN(Id) FROM DataTable");
			int actual = _db.DataTable.Min(d => d.Id);
			Assert.AreEqual(expected, actual);
		}
		
		[Test]
		public void TestMax()
		{
			int expected = _db.ExecuteScalar<int>("SELECT MAX(Id) FROM DataTable");
			int actual = _db.DataTable.Max(d => d.Id);
			Assert.AreEqual(expected, actual);
		}
		
		[Test]
		public void TestSum()
		{
			int expected = _db.ExecuteScalar<int>("SELECT SUM(Id) FROM DataTable");
			int actual = _db.DataTable.Sum(d => d.Id);
			Assert.AreEqual(expected, actual);
		}
		
		[Test]
		public void TestAverage()
		{
			double expected = _db.ExecuteScalar<double>("SELECT AVG(Id) FROM DataTable");
			double actual = _db.DataTable.Average(d => d.Id);
			Assert.AreEqual(expected, actual);
		}
		
		[Test]
		public void TestCount()
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable");
			int actual = _db.DataTable.Count();
			Assert.AreEqual(expected, actual);
		}

		[Test]
		[Category("Distinct")]
		public void TestAggregateDistinct()
		{
			_db.Logger = Console.Out;
			double expected = _db.ExecuteScalar<double>("SELECT AVG(DISTINCT Id / 2) FROM DataTable");
			double actual = _db.DataTable.Select(d => d.Id / 2).Distinct().Average();
			Assert.AreEqual(expected, actual);
		}

		[Test]
		[Category("Where")]
		[TestCase(5, 25)]
		[TestCase(45, 75)]
		[TestCase(37, 40)]
		public void TestCountWhere(int min, int max)
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(Id) FROM DataTable WHERE Id >= ? AND Id <= ?", min, max);
			int actual = _db.DataTable.Count(d => d.Id >= min && d.Id <= max);
			Assert.AreEqual(expected, actual);
		}
	}
}
