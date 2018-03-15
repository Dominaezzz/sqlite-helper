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
		    using (var query = _db.ExecuteQuery("SELECT AlbumId FROM Track GROUP BY AlbumId"))
		    {
			    var result = _db.Tracks.GroupBy(t => t.AlbumId);
			    foreach (var item in result)
			    {
				    Assert.True(query.Step());
				    int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
				    Assert.Equal(expected, item.Key);

				    using (var subQuery = _db.ExecuteQuery("SELECT TrackId FROM Track WHERE AlbumId IS ?", item.Key))
					{
						foreach (var subItem in item)
						{
							Assert.True(subQuery.Step());
							Assert.Equal(subQuery.GetInt(0), subItem.TrackId);
						}
						Assert.False(subQuery.Step());
					}
			    }
			    Assert.False(query.Step());
		    }
		}

	    [Fact]
	    [Trait("Category", "SubIteration")]
		public void TestGroupByCompoundKey()
	    {
		    using (var query = _db.ExecuteQuery("SELECT MediaTypeId, UnitPrice FROM Track GROUP BY MediaTypeId, UnitPrice"))
			{
				var result = _db.Tracks.GroupBy(t => new { t.MediaTypeId, t.UnitPrice });
			    foreach (var item in result)
			    {
					Assert.True(query.Step());
				    Assert.Equal(query.GetInt(0), item.Key.MediaTypeId);
				    Assert.Equal((decimal)query.GetDouble(1), item.Key.UnitPrice);

				    const string subQuerySQL = "SELECT TrackId FROM Track WHERE MediaTypeId IS ? AND UnitPrice IS ?";
					using (var subQuery = _db.ExecuteQuery(subQuerySQL, item.Key.MediaTypeId, item.Key.UnitPrice))
					{
						foreach (var subItem in item)
						{
							Assert.True(subQuery.Step());
							Assert.Equal(subQuery.GetInt(0), subItem.TrackId);
						}
						Assert.False(subQuery.Step());
					}
			    }
			    Assert.False(query.Step());
		    }
		}

		[Fact]
		[Trait("Category", "Aggregate")]
		public void TestGroupByWithAggregate()
		{
			var expected = _db.Query("SELECT SUM(UnitPrice) AS _sum FROM Track GROUP BY AlbumId", row => row.Get<decimal>("_sum"));
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
			var expected = _db.Query("SELECT AlbumId FROM Track GROUP BY AlbumId", row => row.Get<int?>("AlbumId"));
			var actual = _db.Tracks.GroupBy(t => t.AlbumId, (albumId, tracks) => albumId);
			
			Assert.Equal(expected, actual);
		}

	    [Fact]
	    [Trait("Category", "Aggregate")]
		public void TestGroupByThenSelectAggregate()
		{
			var expected = _db.Query("SELECT SUM(UnitPrice) AS _sum FROM Track GROUP BY AlbumId", row => row.Get<decimal>("_sum"));
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
