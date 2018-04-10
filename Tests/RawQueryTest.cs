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
		public void TestQueryDirectPrimitive()
		{
			var result = _db.Query<int>("SELECT 3").Single();

			Assert.Equal(3, result);
		}

		[Fact]
		public void TestQueryDirectTuple()
		{
			var (testLong, testString, testReal) = _db.Query<(long, string, double)>("SELECT 3, 'Hey!', 5.6")
				.Single();

			Assert.Equal(3, testLong);
			Assert.Equal("Hey!", testString);
			Assert.Equal(5.6, testReal);
		}

		[Fact]
		public void TestQueryDynamic()
		{
			var result = _db.Query("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal").Single();

			Assert.Equal(3, result.TestLong);
			Assert.Equal(3, result["TestLong"]);
			Assert.Equal(3, result[0]);

			Assert.Equal("Hey!", result.TestString);
			Assert.Equal("Hey!", result["TestString"]);
			Assert.Equal("Hey!", result[1]);

			Assert.Equal(5.6, result.TestReal);
			Assert.Equal(5.6, result["TestReal"]);
			Assert.Equal(5.6, result[2]);
			
			Assert.ThrowsAny<Exception>(() => result.lololol);
			Assert.ThrowsAny<Exception>(() => result["lololol"]);
			Assert.ThrowsAny<Exception>(() => result[1010101]);
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
