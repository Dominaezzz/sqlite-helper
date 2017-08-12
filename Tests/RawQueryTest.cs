using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture]
    public class RawQueryTest
	{
		private ChinookDatabase _db;
		private class TestClass
		{
			public long TestLong { get; set; }
			public string TestString { get; set; }
			public double TestReal { get; set; }
		}

		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new ChinookDatabase();
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}

		[Test]
		public void TestQueryDirect()
		{
			var result = _db.Query<TestClass>("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal").Single();

			Assert.AreEqual(3, result.TestLong);
			Assert.AreEqual("Hey!", result.TestString);
			Assert.AreEqual(5.6, result.TestReal);
		}

		[Test]
		public void TestQueryDynamic()
		{
			var result = _db.Query("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal").Single();

			Assert.AreEqual(3, result.TestLong);
			Assert.AreEqual("Hey!", result.TestString);
			Assert.AreEqual(5.6, result.TestReal);
		}

		[Test]
		public void TestQueryCustom()
		{
			var result = _db.Query("SELECT 3 AS TestLong, 'Hey!' AS TestString, 5.6 AS TestReal", reader => new TestClass
			{
				TestLong = reader.Get<long>("TestLong"),
				TestString = reader.Get<string>("TestString"),
				TestReal = reader.Get<double>("TestReal")
			})
			.Single();

			Assert.AreEqual(3, result.TestLong);
			Assert.AreEqual("Hey!", result.TestString);
			Assert.AreEqual(5.6, result.TestReal);
		}

		[Test]
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

		[Test]
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
