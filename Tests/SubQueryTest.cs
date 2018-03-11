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
				"SELECT p.Name, (SELECT COUNT(*) FROM PlaylistTrack WHERE PlaylistId == p.PlaylistId) " +
				"FROM Playlist p";
			using (var query = _db.ExecuteQuery(querySQL))
			{
				var result = _db.Playlists.Select(p => new
				{
					Playlist = p.Name,
					TrackCount = _db.PlaylistTracks.Count(pt => pt.PlaylistId == p.PlaylistId)
				});
				foreach (var item in result)
				{
					Assert.True(query.Step());
					Assert.Equal(query.GetText(0), item.Playlist);
					Assert.Equal(query.GetInt(1), item.TrackCount);
				}
				Assert.False(query.Step());
			}
		}
	}
}
