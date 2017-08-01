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
			const string querySQL = "SELECT Album.AlbumId, Track.TrackId " +
									"FROM Track JOIN Album ON Album.AlbumId IS Track.AlbumId";
			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = _db.Albums.Join(_db.Tracks, a => a.AlbumId, t => t.AlbumId, (album, track) => new
				{
					album.AlbumId,
					track.TrackId
				});
				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetInt(0), item.AlbumId);
					Assert.AreEqual(query.GetInt(1), item.TrackId);
				}
				Assert.IsFalse(query.Step());
			}
		}

		[Test]
		public void TestJoinNested()
		{
			const string querySQL = "SELECT Artist.ArtistId, Album.AlbumId, Track.TrackId " +
									"FROM Artist " +
									"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
									"JOIN Track ON Track.AlbumId IS Album.AlbumId";

			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = from artist in _db.Artists
					join album in _db.Albums on artist.ArtistId equals album.ArtistId
					join track in _db.Tracks on album.AlbumId equals track.AlbumId
					select new { artist.ArtistId, album.AlbumId, track.TrackId };

				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetInt(0), item.ArtistId);
					Assert.AreEqual(query.GetInt(1), item.AlbumId);
					Assert.AreEqual(query.GetInt(2), item.TrackId);
				}
				Assert.IsFalse(query.Step());
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("Aggregate")]
		public void TestJoinThenGroupBy()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, MIN(Artist.Name), COUNT(*) " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";

			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = _db.Artists.Join(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, album) => new
					{
						Artist = artist,
						Album = album
					})
					.GroupBy(p => p.Artist.ArtistId, (id, enumerable) => new
					{
						ArtistId = id,
						ArtistName = enumerable.Min(aa => aa.Artist.Name),
						AlbumCount = enumerable.Count()
					});
				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetInt(0), item.ArtistId);
					Assert.AreEqual(query.GetText(1), item.ArtistName);
					Assert.AreEqual(query.GetInt(2), item.AlbumCount);
				}
				Assert.IsFalse(query.Step());
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("SubIteration")]
		public void TestGroupJoin()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, Album.AlbumId " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";

			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
				{
					Artist = artist,
					Albums = albums
				});
				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetInt(0), item.Artist.ArtistId);

					using (var subQuery = _db.ExecuteQuery("SELECT AlbumId FROM Album WHERE ArtistId IS ?", item.Artist.ArtistId))
					{
						foreach (var subItem in item.Albums)
						{
							Assert.IsTrue(subQuery.Step());
							Assert.AreEqual(subQuery.GetInt(0), subItem.AlbumId);
						}
						Assert.IsFalse(subQuery.Step());
					}
				}
				Assert.IsFalse(query.Step());
			}
		}

		[Test]
		[Category("GroupBy")]
		[Category("Aggregate")]
		public void TestGroupJoinWithAggregate()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, Artist.Name, COUNT(*) " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";

			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
				{
					ArtistId = artist.ArtistId,
					ArtistName = artist.Name,
					AlbumCount = albums.Count()
				});
				foreach (var item in result)
				{
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetInt(0), item.ArtistId);
					Assert.AreEqual(query.GetText(1), item.ArtistName);
					Assert.AreEqual(query.GetInt(2), item.AlbumCount);
				}
				Assert.IsFalse(query.Step());
			}
		}
	}
}
