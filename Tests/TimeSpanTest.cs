using System;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "TimeSpan")]
	public class TimeSpanTest : IDisposable
	{
		private readonly SQLiteDb<TimeSpan> _db;
		private readonly TimeSpan _testTimeSpan = new TimeSpan(10, 2, 30, 4, 567).Add(TimeSpan.FromTicks(100));
		private readonly TimeSpan _testTimeSpan2 = new TimeSpan(5, 6, 20, 9, 713).Add(TimeSpan.FromTicks(300));

		public TimeSpanTest()
		{
			_db = new SQLiteDb<TimeSpan>();
			_db.DataTable.Insert(new Data<TimeSpan> { Value = _testTimeSpan });
		}

		public void Dispose()
		{
			_db.Dispose();
		}


		[Fact]
		public void TestTotalDays()
		{
			Assert.Equal(_testTimeSpan.TotalDays, _db.DataTable.Select(d => d.Value.TotalDays).Single());
		}

		[Fact]
		public void TestTotalHours()
		{
			Assert.Equal(_testTimeSpan.TotalHours, _db.DataTable.Select(d => d.Value.TotalHours).Single(), 10);
		}

		[Fact]
		public void TestTotalMinutes()
		{
			Assert.Equal(_testTimeSpan.TotalMinutes, _db.DataTable.Select(d => d.Value.TotalMinutes).Single());
		}

		[Fact]
		public void TestTotalSeconds()
		{
			Assert.Equal(_testTimeSpan.TotalSeconds, _db.DataTable.Select(d => d.Value.TotalSeconds).Single(), 10);
		}

		[Fact]
		public void TestTotalMilliSeconds()
		{
			Assert.Equal(_testTimeSpan.TotalMilliseconds, _db.DataTable.Select(d => d.Value.TotalMilliseconds).Single());
		}


		[Fact]
		public void TestDays()
		{
			Assert.Equal(_testTimeSpan.Days, _db.DataTable.Select(d => d.Value.Days).Single());
		}

		[Fact]
		public void TestHours()
		{
			Assert.Equal(_testTimeSpan.Hours, _db.DataTable.Select(d => d.Value.Hours).Single());
		}

		[Fact]
		public void TestMinutes()
		{
			Assert.Equal(_testTimeSpan.Minutes, _db.DataTable.Select(d => d.Value.Minutes).Single());
		}

		[Fact]
		public void TestSeconds()
		{
			Assert.Equal(_testTimeSpan.Seconds, _db.DataTable.Select(d => d.Value.Seconds).Single());
		}

		[Fact]
		public void TestMilliSeconds()
		{
			Assert.Equal(_testTimeSpan.Milliseconds, _db.DataTable.Select(d => d.Value.Milliseconds).Single());
		}

		[Fact]
		public void TestTicks()
		{
			Assert.Equal(_testTimeSpan.Ticks, _db.DataTable.Select(d => d.Value.Ticks).Single());
		}


		[Theory]
		[InlineData(13.034)]
		public void TestFromDays(double value)
		{
			Assert.Equal(
				TimeSpan.FromDays(value), _db.DataTable.Select(d => TimeSpan.FromDays(d.Id * value)).Single()
			);
		}

		[Theory]
		[InlineData(13.034)]
		public void TestFromHours(double value)
		{
			Assert.Equal(
				TimeSpan.FromHours(value), _db.DataTable.Select(d => TimeSpan.FromHours(d.Id * value)).Single()
			);
		}

		[Theory]
		[InlineData(13.034)]
		public void TestFromMinutes(double value)
		{
			Assert.Equal(
				TimeSpan.FromMinutes(value), _db.DataTable.Select(d => TimeSpan.FromMinutes(d.Id * value)).Single()
			);
		}

		[Theory]
		[InlineData(13.034)]
		public void TestFromSeconds(double value)
		{
			Assert.Equal(
				TimeSpan.FromSeconds(value), _db.DataTable.Select(d => TimeSpan.FromSeconds(d.Id * value)).Single()
			);
		}

		[Theory]
		[InlineData(13.0)]
		public void TestFromMilliSeconds(double value)
		{
			Assert.Equal(
				TimeSpan.FromMilliseconds(value),
				_db.DataTable.Select(d => TimeSpan.FromMilliseconds(d.Id * value)).Single()
			);
		}

		[Theory]
		[InlineData(123456789)]
		public void TestFromTicks(long value)
		{
			Assert.Equal(
				TimeSpan.FromTicks(value),
				_db.DataTable.Select(d => TimeSpan.FromTicks(d.Id * value)).Single()
			);
		}


		[Fact]
		public void TestAdd()
		{
			Assert.Equal(
				_testTimeSpan.Add(_testTimeSpan2),
				_db.DataTable.Select(d => d.Value.Add(_testTimeSpan2)).Single()
			);
		}

		[Fact]
		public void TestSubtract()
		{
			Assert.Equal(
				_testTimeSpan.Subtract(_testTimeSpan2),
				_db.DataTable.Select(d => d.Value.Subtract(_testTimeSpan2)).Single()
			);
		}

		[Fact]
		public void TestNegate()
		{
			Assert.Equal(_testTimeSpan.Negate(), _db.DataTable.Select(d => d.Value.Negate()).Single());
		}

		[Fact]
		public void TestDuration()
		{
			Assert.Equal(_testTimeSpan.Duration(), _db.DataTable.Select(d => d.Value.Duration()).Single());
		}
	}
}
