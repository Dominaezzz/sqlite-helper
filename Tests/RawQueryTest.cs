using System;
using System.Linq;
using Xunit;

namespace Tests
{
    public class RawQueryTest : IDisposable
	{
		private readonly ChinookDatabase _db;
		private class TestClass
		{
			public long TestLong { get; set; }
			public string TestString { get; set; }
			public double TestReal { get; set; }
		}

		public RawQueryTest()
		{
			_db = new ChinookDatabase();
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
		public void TestQueryDirect()
		{
			var result = _db.Query<TestClass>("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal").Single();

			Assert.Equal(3, result.TestLong);
			Assert.Equal("Hey!", result.TestString);
			Assert.Equal(5.6, result.TestReal);
		}

		[Fact]
		public void TestQueryDynamic()
		{
			var result = _db.Query("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal").Single();

			Assert.Equal(3, result.TestLong);
			Assert.Equal("Hey!", result.TestString);
			Assert.Equal(5.6, result.TestReal);
		}

		[Fact]
		public void TestQueryCustom()
		{
			var result = _db.Query("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal", reader => new TestClass
			{
				TestLong = reader.Get<long>("TestLong"),
				TestString = reader.Get<string>("TestString"),
				TestReal = reader.Get<double>("TestReal")
			})
			.Single();

			Assert.Equal(3, result.TestLong);
			Assert.Equal("Hey!", result.TestString);
			Assert.Equal(5.6, result.TestReal);
		}
	}
}
