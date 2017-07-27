using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;

namespace Tests
{
	[TestFixture(Category = "GroupBy")]
    public class GroupByTest
    {
	    private ChinookDatabase _db;

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
		[Category("SubIteration")]
	    public void TestGroupBy()
	    {
		    using (var query = _db.ExecuteQuery("SELECT AlbumId FROM Track GROUP BY AlbumId"))
		    {
			    var result = _db.Tracks.GroupBy(t => t.AlbumId);
			    foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
				    Assert.AreEqual(expected, item.Key);

				    using (var subQuery = _db.ExecuteQuery("SELECT TrackId FROM Track WHERE AlbumId IS ?", item.Key))
					{
						foreach (var subItem in item)
						{
							Assert.IsTrue(subQuery.Step());
							Assert.AreEqual(subQuery.GetInt(0), subItem.TrackId);
						}
						Assert.IsFalse(subQuery.Step());
					}
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

	    [Test]
	    [Category("SubIteration")]
		public void TestGroupByCompoundKey()
	    {
		    using (var query = _db.ExecuteQuery("SELECT MediaTypeId, UnitPrice FROM Track GROUP BY MediaTypeId, UnitPrice"))
			{
				var result = _db.Tracks.GroupBy(t => new { t.MediaTypeId, t.UnitPrice });
			    foreach (var item in result)
			    {
					Assert.IsTrue(query.Step());
				    Assert.AreEqual(query.GetInt(0), item.Key.MediaTypeId);
				    Assert.AreEqual((decimal)query.GetDouble(1), item.Key.UnitPrice);

				    const string subQuerySQL = "SELECT TrackId FROM Track WHERE MediaTypeId IS ? AND UnitPrice IS ?";
					using (var subQuery = _db.ExecuteQuery(subQuerySQL, item.Key.MediaTypeId, item.Key.UnitPrice))
					{
						foreach (var subItem in item)
						{
							Assert.IsTrue(subQuery.Step());
							Assert.AreEqual(subQuery.GetInt(0), subItem.TrackId);
						}
						Assert.IsFalse(subQuery.Step());
					}
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

		[Test]
		[Category("Aggregate")]
		public void TestGroupByWithAggregate()
	    {
		    using (var query = _db.ExecuteQuery("SELECT SUM(UnitPrice) FROM Track GROUP BY AlbumId"))
			{
				var result = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => tracks.Sum(t => t.UnitPrice));
			    foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    Assert.AreEqual((decimal)query.GetDouble(0), item);
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

		[Test]
		[Category("Aggregate")]
		public void TestGroupByWithAggregates()
		{
			using (var query = _db.ExecuteQuery("SELECT AlbumId, SUM(UnitPrice), COUNT(*) FROM Track GROUP BY AlbumId"))
			{
				var result = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => new
				{
					AlbumId = id,
					TotalPrice = tracks.Sum(t => t.UnitPrice),
					Count = tracks.Count()
				});
				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
					Assert.AreEqual(expected, item.AlbumId);
					Assert.AreEqual((decimal)query.GetDouble(1), item.TotalPrice);
					Assert.AreEqual(query.GetInt(2), item.Count);
				}
				Assert.IsFalse(query.Step());
			}
		}

	    [Test]
	    public void TestGroupByWithSelect()
	    {
		    using (var query = _db.ExecuteQuery("SELECT AlbumId FROM Track GROUP BY AlbumId"))
			{
				var result = _db.Tracks.GroupBy(t => t.AlbumId, (albumId, tracks) => albumId);
			    foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
				    Assert.AreEqual(expected, item);
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByThenSelectAggregate()
	    {
		    using (var query = _db.ExecuteQuery("SELECT SUM(UnitPrice) FROM Track GROUP BY AlbumId"))
			{
				var result = _db.Tracks.GroupBy(t => t.AlbumId).Select(g => g.Sum(t => t.UnitPrice));
			    foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    Assert.AreEqual((decimal)query.GetDouble(0), item);
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByWithSelectedAggregates()
	    {
		    using (var query = _db.ExecuteQuery("SELECT AlbumId, SUM(UnitPrice), COUNT(*) FROM Track GROUP BY AlbumId"))
		    {
			    var result = _db.Tracks.GroupBy(t => t.AlbumId).Select(g => new
			    {
				    AlbumId = g.Key,
				    TotalPrice = g.Sum(t => t.UnitPrice),
				    Count = g.Count()
			    });
				foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
				    Assert.AreEqual(expected, item.AlbumId);
				    Assert.AreEqual((decimal)query.GetDouble(1), item.TotalPrice);
				    Assert.AreEqual(query.GetInt(2), item.Count);
			    }
			    Assert.IsFalse(query.Step());
		    }
		}

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByWithHavingAndSelectedAggregates()
	    {
		    const string querySQL =
			    "SELECT AlbumId, SUM(UnitPrice), COUNT(*) FROM Track GROUP BY AlbumId HAVING AVG(Milliseconds) >= 20000";

			using (var query = _db.ExecuteQuery(querySQL))
		    {
				var result = _db.Tracks.GroupBy(t => t.AlbumId)
					.Where(g => g.Average(t => t.Milliseconds) >= 20000)
					.Select(g => new
					{
						AlbumId = g.Key,
						TotalPrice = g.Sum(t => t.UnitPrice),
						Count = g.Count()
					});
				foreach (var item in result)
			    {
				    Assert.IsTrue(query.Step());
				    int? expected = query.IsNull(0) ? (int?)null : query.GetInt(0);
				    Assert.AreEqual(expected, item.AlbumId);
				    Assert.AreEqual((decimal)query.GetDouble(1), item.TotalPrice);
				    Assert.AreEqual(query.GetInt(2), item.Count);
			    }
			    Assert.IsFalse(query.Step());
		    }
		}
	}
}
