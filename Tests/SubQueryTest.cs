using System;
using System.Linq;
using Xunit;

namespace Tests
{
    public class SubQueryTest : IDisposable
	{
		private readonly ChinookDatabase _db;

		public SubQueryTest()
		{
			_db = new ChinookDatabase();
		}

		public void Dispose()
		{
			_db.Dispose();
		}


		[Fact]
		public void TestSimpleSubQuery()
		{
			const string querySQL =
				"SELECT p.Name, (SELECT COUNT(*) FROM PlaylistTrack WHERE PlaylistId == p.PlaylistId) AS _count " +
				"FROM Playlist p";

			var expected = _db.Query(querySQL, row => new
			{
				Playlist = row.Get<string>("Name"),
				TrackCount = row.Get<int>("_count")
			});
			var actual = _db.Playlists.Select(p => new
			{
				Playlist = p.Name,
				TrackCount = _db.PlaylistTracks.Count(pt => pt.PlaylistId == p.PlaylistId)
			});
			
			Assert.Equal(expected, actual);
		}
	}
}
