using System;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "GroupBy")]
    public class GroupByTest : IDisposable
    {
	    private readonly ChinookDatabase _db;

	    public GroupByTest()
	    {
		    _db = new ChinookDatabase();
	    }

	    public void Dispose()
	    {
		    _db.Dispose();
	    }
		

		[Fact]
		[Trait("Category", "SubIteration")]
	    public void TestGroupBy()
		{
			var expectedResult = _db.Query<int?>("SELECT AlbumId FROM Track GROUP BY AlbumId");
			var actualResult = _db.Tracks.GroupBy(t => t.AlbumId);

			foreach (var (expected, item) in expectedResult.Zip(actualResult, (e, a) => (e, a)))
			{
				Assert.Equal(expected, item.Key);

				var expectedSubResult = _db.Query<int>("SELECT TrackId FROM Track WHERE AlbumId IS ?", item.Key);
				
				foreach (var (subExpected, actual) in expectedSubResult.Zip(item, (e, a) => (e, a)))
				{
					Assert.Equal(subExpected, actual.TrackId);
				}
			}
		}

	    [Fact]
	    [Trait("Category", "SubIteration")]
		public void TestGroupByCompoundKey()
	    {
			var querySQL = "SELECT MediaTypeId, UnitPrice FROM Track GROUP BY MediaTypeId, UnitPrice";

			var expected = _db.Query<(int MediaTypeId, decimal UnitPrice)>(querySQL);
			var result = _db.Tracks.GroupBy(t => new { t.MediaTypeId, t.UnitPrice });

			foreach (var (exp, item) in expected.Zip(result, (e, a) => (e, a)))
			{
				Assert.Equal(exp.MediaTypeId, item.Key.MediaTypeId);
				Assert.Equal(exp.UnitPrice, item.Key.UnitPrice);

				const string subQuerySQL = "SELECT TrackId FROM Track WHERE MediaTypeId IS ? AND UnitPrice IS ?";
				var subExpected = _db.Query<int>(subQuerySQL, item.Key.MediaTypeId, item.Key.UnitPrice);
				foreach (var (expectedTrackId, subItem) in subExpected.Zip(item, (e, a) => (e, a)))
				{
					Assert.Equal(expectedTrackId, subItem.TrackId);
				}
			}
		}

		[Fact]
		[Trait("Category", "Aggregate")]
		public void TestGroupByWithAggregate()
		{
			var expected = _db.Query<decimal>("SELECT SUM(UnitPrice) FROM Track GROUP BY AlbumId");
			var actual = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => tracks.Sum(t => t.UnitPrice));
			
			Assert.Equal(expected, actual);
		}

		[Fact]
		[Trait("Category", "Aggregate")]
		public void TestGroupByWithAggregates()
		{
			var expected = _db.Query("SELECT AlbumId, SUM(UnitPrice) AS _sum, COUNT(*) AS _cnt FROM Track GROUP BY AlbumId", row => new
			{
				AlbumId = row.Get<int?>("AlbumId"),
				TotalPrice = row.Get<decimal>("_sum"),
				Count = row.Get<int>("_cnt")
			});
			var actual = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => new
			{
				AlbumId = id,
				TotalPrice = tracks.Sum(t => t.UnitPrice),
				Count = tracks.Count()
			});
			
			Assert.Equal(expected, actual);
		}

	    [Fact]
	    public void TestGroupByWithSelect()
		{
			var expected = _db.Query<int?>("SELECT AlbumId FROM Track GROUP BY AlbumId");
			var actual = _db.Tracks.GroupBy(t => t.AlbumId, (albumId, tracks) => albumId);
			
			Assert.Equal(expected, actual);
		}

	    [Fact]
	    [Trait("Category", "Aggregate")]
		public void TestGroupByThenSelectAggregate()
		{
			var expected = _db.Query<decimal>("SELECT SUM(UnitPrice) FROM Track GROUP BY AlbumId");
			var actual = _db.Tracks.GroupBy(t => t.AlbumId).Select(g => g.Sum(t => t.UnitPrice));
		    
			Assert.Equal(expected, actual);
		}

	    [Fact]
	    [Trait("Category", "Aggregate")]
		public void TestGroupByWithSelectedAggregates()
		{
			var expected = _db.Query("SELECT AlbumId, SUM(UnitPrice) AS _sum, COUNT(*) AS _cnt FROM Track GROUP BY AlbumId", row => new
			{
				AlbumId = row.Get<int?>("AlbumId"),
				TotalPrice = row.Get<decimal>("_sum"),
				Count = row.Get<int>("_cnt")
			});
			var actual = _db.Tracks.GroupBy(t => t.AlbumId).Select(g => new
			{
				AlbumId = g.Key,
				TotalPrice = g.Sum(t => t.UnitPrice),
				Count = g.Count()
			});
			
			Assert.Equal(expected, actual);
		}

	    [Fact]
	    [Trait("Category", "Aggregate")]
		public void TestGroupByWithHavingAndSelectedAggregates()
	    {
		    const string querySQL =
			    "SELECT AlbumId, SUM(UnitPrice) AS _sum, COUNT(*) AS _cnt FROM Track GROUP BY AlbumId HAVING AVG(Milliseconds) >= 20000";

			var expected = _db.Query(querySQL, row => new
			{
				AlbumId = row.Get<int?>("AlbumId"),
				TotalPrice = row.Get<decimal>("_sum"),
				Count = row.Get<int>("_cnt")
			});
			var actual = _db.Tracks.GroupBy(t => t.AlbumId)
				.Where(g => g.Average(t => t.Milliseconds) >= 20000)
				.Select(g => new
				{
					AlbumId = g.Key,
					TotalPrice = g.Sum(t => t.UnitPrice),
					Count = g.Count()
				});

			Assert.Equal(expected, actual);
		}
	}
}
