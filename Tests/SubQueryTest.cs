using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture]
    public class SubQueryTest
	{
		private ChinookDatabase _db;

		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new ChinookDatabase();
			_db.Logger = Console.Out;
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}


		[Test]
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
					Assert.IsTrue(query.Step());
					Assert.AreEqual(query.GetText(0), item.Playlist);
					Assert.AreEqual(query.GetInt(1), item.TrackCount);
				}
				Assert.IsFalse(query.Step());
			}
		}
	}
}
