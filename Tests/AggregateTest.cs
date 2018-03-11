using System;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "Aggregate")]
    public class AggregateTest : IDisposable
	{
		private const int RowCount = 100;

		private readonly SQLiteDb<string> _db;

		public AggregateTest()
		{
			_db = new SQLiteDb<string>();
			_db.DataTable.Insert(Enumerable.Range(0, RowCount).Select(i => new Data<string> { Value = $"Some Text {i}" }));
		}

		public void Dispose()
		{
			_db.Dispose();
		}


		[Fact]
		public void TestMin()
		{
			int expected = _db.ExecuteScalar<int>("SELECT MIN(Id) FROM DataTable");
			int actual = _db.DataTable.Min(d => d.Id);
			Assert.Equal(expected, actual);
		}
		
		[Fact]
		public void TestMax()
		{
			int expected = _db.ExecuteScalar<int>("SELECT MAX(Id) FROM DataTable");
			int actual = _db.DataTable.Max(d => d.Id);
			Assert.Equal(expected, actual);
		}
		
		[Fact]
		public void TestSum()
		{
			int expected = _db.ExecuteScalar<int>("SELECT SUM(Id) FROM DataTable");
			int actual = _db.DataTable.Sum(d => d.Id);
			Assert.Equal(expected, actual);
		}
		
		[Fact]
		public void TestAverage()
		{
			double expected = _db.ExecuteScalar<double>("SELECT AVG(Id) FROM DataTable");
			double actual = _db.DataTable.Average(d => d.Id);
			Assert.Equal(expected, actual);
		}
		
		[Fact]
		public void TestCount()
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(*) FROM DataTable");
			int actual = _db.DataTable.Count();
			Assert.Equal(expected, actual);
		}

		[Fact]
		[Trait("Category", "Distinct")]
		public void TestAggregateDistinct()
		{
			double expected = _db.ExecuteScalar<double>("SELECT AVG(DISTINCT Id / 2) FROM DataTable");
			double actual = _db.DataTable.Select(d => d.Id / 2).Distinct().Average();
			Assert.Equal(expected, actual);
		}

		[Theory]
		[Trait("Category", "Where")]
		[InlineData(5, 25)]
		[InlineData(45, 75)]
		[InlineData(37, 40)]
		public void TestCountWhere(int min, int max)
		{
			int expected = _db.ExecuteScalar<int>("SELECT COUNT(Id) FROM DataTable WHERE Id >= ? AND Id <= ?", min, max);
			int actual = _db.DataTable.Count(d => d.Id >= min && d.Id <= max);
			Assert.Equal(expected, actual);
		}
	}
}
