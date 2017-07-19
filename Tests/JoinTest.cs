using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture(Category = "Join")]
    public class JoinTest
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
		public void TestJoin()
		{
			var result = _db.Albums.Join(_db.Tracks, a => a.AlbumId, t => t.AlbumId, (album, track) => new
			{
				album.AlbumId,
				track.TrackId
			});

			foreach (var item in result)
			{
				Console.WriteLine(item);
			}
		}

		[Test]
		public void TestJoinNested()
		{
			var result = _db.Artists.Join(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, album) => new
				{
					artist.ArtistId,
					album.AlbumId
				})
				.Join(_db.Tracks, arg => arg.AlbumId, track => track.AlbumId, (arg, track) => new
				{
					arg.ArtistId,
					arg.AlbumId,
					track.TrackId
				});

			foreach (var item in result)
			{
				Console.WriteLine(item);
			}
		}

		[Test]
		[Category("Where")]
		public void TestJoinWithWhere()
		{
			var result = _db.Albums.Join(_db.Tracks, a => a.AlbumId, t => t.AlbumId, (album, track) => new
			{
				album.AlbumId,
				track.TrackId
			})
			.Where(ai => ai.AlbumId != ai.TrackId);

			foreach (var item in result)
			{
				Console.WriteLine(item);
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("SubIteration")]
		public void TestJoinWithGroupBy()
		{
			var result = _db.Artists.Join(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, album) => new
				{
					Artist = artist,
					Album = album
				})
				.GroupBy(d => d.Artist.ArtistId);

			foreach (var item in result)
			{
				bool done = false;
				foreach (var subItem in item)
				{
					if (!done)
					{
						Console.WriteLine(subItem.Artist.Name);
						done = true;
					}
					Console.WriteLine($"\t{subItem.Album.Title}");
				}
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("SubIteration")]
		public void TestGroupJoin()
		{
			var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
			{
				Artist = artist,
				Albums = albums
			});

			foreach (var item in result)
			{
				Console.WriteLine(item.Artist.Name);
				foreach (var album in item.Albums)
				{
					Console.WriteLine($"\t{album.Title}");
				}
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("Aggregate")]
		public void TestGroupJoinWithAggragate()
		{
			var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
			{
				Artist = artist.Name,
				AlbumCount = albums.Count()
			});

			foreach (var item in result)
			{
				Console.WriteLine(item);
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("Aggregate")]
		public void TestGroupJoinWithNestedAggragates()
		{
			var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
			{
				Artist = artist.Name,
				AlbumCount = albums.Count(),
				UnitPrice = albums.Sum(a => _db.Tracks.Where(t => t.AlbumId == a.AlbumId).Sum(t => t.UnitPrice))
			});

			foreach (var item in result)
			{
				Console.WriteLine(item);
			}
		}
	}
}
