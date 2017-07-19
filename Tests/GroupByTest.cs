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
	    public void TestGroupBy()
		{
			var result = _db.Tracks.GroupBy(t => t.AlbumId);
			
			foreach (var item in result)
			{
				Console.WriteLine(item.Key);
			}
		}

	    [Test]
	    public void TestGroupByWithCompoundKey()
	    {
		    var result = _db.Tracks.GroupBy(t => new{ t.MediaTypeId, t.UnitPrice });

			foreach (var item in result)
			{
				Console.WriteLine(item.Key);
			}
		}

		[Test]
		[Category("SubIteration")]
	    public void TestGroupByWithIteration()
	    {
		    var result = _db.Tracks.GroupBy(t => t.AlbumId);

		    foreach (var item in result)
		    {
			    Console.WriteLine(item.Key);

			    foreach (var subItem in item)
			    {
				    Console.WriteLine($"\tTitle - {subItem.Name}, Price - {subItem.UnitPrice}");
			    }
		    }
		}

	    [Test]
	    [Category("SubIteration")]
		public void TestGroupByWithCompoundKeyAndIteration()
	    {
		    var result = _db.Tracks.GroupBy(t => new { t.MediaTypeId, t.UnitPrice });

		    foreach (var item in result)
		    {
			    Console.WriteLine(item.Key);

			    foreach (var subItem in item)
			    {
				    Console.WriteLine($"\tTitle - {subItem.Name}, Price - {subItem.UnitPrice}");
			    }
		    }
	    }

		[Test]
		[Category("Aggregate")]
		public void TestGroupByWithAggregate()
	    {
		    var result = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => tracks.Sum(t => t.UnitPrice));

		    foreach (var item in result)
		    {
			    Console.WriteLine($"Price - {item}");
		    }
		}

		[Test]
		[Category("Aggregate")]
		public void TestGroupByWithAggregates()
		{
			var result = _db.Tracks.GroupBy(t => t.AlbumId, (id, tracks) => new
			{
				AlbumId = id,
				TotalPrice = tracks.Sum(t => t.UnitPrice),
				Count = tracks.Count()
			});

		    foreach (var item in result)
		    {
			    Console.WriteLine(item);
		    }
		}

		[Test]
		[Category("Where")]
		public void TestGroupByWithWhere()
	    {
			var result = _db.Tracks
				.Where(t => t.AlbumId != null)
				.GroupBy(t => t.AlbumId);

			foreach (var item in result)
			{
				Console.WriteLine(item.Key);
			}
		}

	    [Test]
	    public void TestGroupByWithSelect()
	    {
		    var result = _db.Tracks
			    .GroupBy(t => t.AlbumId)
				.Select(g => g.Key);

		    foreach (var item in result)
		    {
			    Console.WriteLine(item);
		    }
	    }

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByWithSelectedAggregate()
	    {
		    var result = _db.Tracks.GroupBy(t => t.AlbumId).Select(g => g.Sum(t => t.UnitPrice));

		    foreach (var item in result)
		    {
			    Console.WriteLine($"Price - {item}");
		    }
	    }

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByWithSelectedAggregates()
	    {
		    var result = _db.Tracks.GroupBy(t => t.AlbumId)
			    .Select(g => new
			    {
				    AlbumId = g.Key,
				    TotalPrice = g.Sum(t => t.UnitPrice),
				    Count = g.Count()
			    });

		    foreach (var item in result)
		    {
			    Console.WriteLine(item);
		    }
		}

	    [Test]
	    [Category("Aggregate")]
		public void TestGroupByWithHavingAndSelectedAggregates()
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
			    Console.WriteLine(item);
		    }
	    }
	}
}
