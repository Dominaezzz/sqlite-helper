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

		[Fact]
		public void TestPragmaTableQuery()
		{
			foreach (var table in _db.SQLiteMaster.Where(dbo => dbo.Type == "table"))
			{
				Console.WriteLine();
				var result = _db.Query($"PRAGMA table_info([{table.Name}]);", reader => new
				{
					CId = reader.Get<int>("cid"),
					Name = reader.Get<string>("name"),
					Type = reader.Get<string>("type"),
					NotNull = reader.Get<bool>("notnull"),
					Default = reader.Get<object>("dflt_value"),
					PrimaryKey = reader.Get<bool>("pk")
				});
				foreach (var column in result)
				{
					Console.WriteLine($"\t{column}");
				}
			}
		}

		[Fact]
		public void TestPragmaIndexQuery()
		{
			foreach (var table in _db.SQLiteMaster.Where(dbo => dbo.Type == "table"))
			{
				Console.WriteLine();
				var indices = _db.Query($"PRAGMA index_list([{table.Name}]);", reader => new
				{
					Sequence = reader.Get<int>("seq"),
					Name = reader.Get<string>("name"),
					Unique = reader.Get<bool>("unique"),
					Origin = reader.Get<string>("origin"),
					Partial = reader.Get<bool>("partial")
				});

				foreach (var index in indices)
				{
					Console.WriteLine($"\t{index}");

					Console.Write("\t\t");
					var indexColumns = _db.Query($"PRAGMA index_info([{index.Name}]);", reader => new
					{
						No = reader.Get<int>("seqno"),
						CId = reader.Get<int>("cid"),
						Name = reader.Get<string>("name")
					});
					foreach (var indexColumn in indexColumns)
					{
						Console.WriteLine($"\t\t\t{indexColumn}");
					}

					Console.WriteLine();

					Console.Write("\t\t");
					var xIndexColumns = _db.Query($"PRAGMA index_xinfo([{index.Name}]);", reader => new
					{
						No = reader.Get<int>("seqno"),
						CId = reader.Get<int>("cid"),
						Name = reader.Get<string>("name"),
						Desc = reader.Get<bool>("desc"),
						Collation = reader.Get<string>("coll"),
						Key = reader.Get<bool>("key")
					});
					foreach (var indexColumn in xIndexColumns)
					{
						Console.WriteLine($"\t\t\t{indexColumn}");
					}
				}
			}
		}
	}
}
