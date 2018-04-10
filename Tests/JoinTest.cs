using System;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "Join")]
    public class JoinTest : IDisposable
	{
		private readonly ChinookDatabase _db;

		public JoinTest()
		{
			_db = new ChinookDatabase();
		}

		public void Dispose()
		{
			_db.Dispose();
		}


		[Fact]
		public void TestJoin()
		{
			const string querySQL = "SELECT Album.AlbumId, Track.TrackId " +
									"FROM Track JOIN Album ON Album.AlbumId IS Track.AlbumId";

			var expected = _db.Query(querySQL, row => new
			{
				AlbumId = row.Get<int>("AlbumId"),
				TrackId = row.Get<int>("TrackId")
			});
			var actual = _db.Albums.Join(_db.Tracks, a => a.AlbumId, t => t.AlbumId, (album, track) => new
			{
				album.AlbumId,
				track.TrackId
			});
			
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void TestJoinNested()
		{
			const string querySQL = "SELECT Artist.ArtistId, Album.AlbumId, Track.TrackId " +
									"FROM Artist " +
									"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
									"JOIN Track ON Track.AlbumId IS Album.AlbumId";

			var expected = _db.Query(querySQL, row => new
			{
				ArtistId = row.Get<int>("ArtistId"),
				AlbumId = row.Get<int>("AlbumId"),
				TrackId = row.Get<int>("TrackId")
			});
			var actual = from artist in _db.Artists
				join album in _db.Albums on artist.ArtistId equals album.ArtistId
				join track in _db.Tracks on album.AlbumId equals track.AlbumId
				select new { artist.ArtistId, album.AlbumId, track.TrackId };
			
			Assert.Equal(expected, actual);
		}

		[Fact]
		[Trait("Category", "GroupBy")]
		[Trait("Category", "Aggregate")]
		public void TestJoinThenGroupBy()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, MIN(Artist.Name) AS _min, COUNT(*) AS _cnt " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";

			var expected = _db.Query(querySQL, row => new
			{
				ArtistId = row.Get<int>("ArtistId"),
				ArtistName = row.Get<string>("_min"),
				AlbumCount = row.Get<int>("_cnt")
			});
			var actual = _db.Artists.Join(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, album) => new
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
			
			Assert.Equal(expected, actual);
		}

		[Fact]
		[Trait("Category", "GroupBy")]
		[Trait("Category", "SubIteration")]
		public void TestGroupJoin()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, Album.AlbumId " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";
				
			var expected = _db.Query<int>(querySQL);
			var result = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
			{
				Artist = artist,
				Albums = albums
			});
			foreach (var (expectedArtistId, item) in expected.Zip(result, (e, r) => (e, r)))
			{
				Assert.Equal(expectedArtistId, item.Artist.ArtistId);

				var subExpected = _db.Query<int>("SELECT AlbumId FROM Album WHERE ArtistId IS ?", item.Artist.ArtistId);
				var subActual = item.Albums.Select(a => a.AlbumId);

				Assert.Equal(subExpected, subActual);
			}
		}

		[Fact]
		[Trait("Category", "GroupBy")]
		[Trait("Category", "Aggregate")]
		public void TestGroupJoinWithAggregate()
		{
			const string querySQL =
				"SELECT Artist.ArtistId, Artist.Name, COUNT(*) AS _cnt " +
				"FROM Artist " +
				"JOIN Album ON Album.ArtistId IS Artist.ArtistId " +
				"GROUP BY Artist.ArtistId";

			var expected = _db.Query(querySQL, row => new
			{
				ArtistId = row.Get<int>("ArtistId"),
				ArtistName = row.Get<string>("Name"),
				AlbumCount = row.Get<int>("_cnt")
			});
			var actual = _db.Artists.GroupJoin(_db.Albums, artist => artist.ArtistId, album => album.ArtistId, (artist, albums) => new
			{
				ArtistId = artist.ArtistId,
				ArtistName = artist.Name,
				AlbumCount = albums.Count()
			});
			
			Assert.Equal(expected, actual);
		}
	}
}
