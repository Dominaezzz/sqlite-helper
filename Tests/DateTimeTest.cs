using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Tests
{
	[Trait("Category", "DateTime")]
    public class DateTimeTest : IDisposable
	{
		private readonly SQLiteDb<DateTime> _db;
		private readonly DateTime _testDate = new DateTime(2017, 7, 13, 9, 55, 18, 123);
		
		public DateTimeTest()
		{
			_db = new SQLiteDb<DateTime>();
			_db.DataTable.Insert(new Data<DateTime> { Value = _testDate });
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		public static IEnumerable<object[]> EnumerateValues(int from, int to)
		{
			return Enumerable.Range(from, to - from).Select(i => new object[]{ i });
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddYears(int years)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddYears(years)).Single();

			Assert.Equal(_testDate.AddYears(years), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddMonths(int months)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMonths(months)).Single();

			Assert.Equal(_testDate.AddMonths(months), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddDays(int days)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddDays(days)).Single();

			Assert.Equal(_testDate.AddDays(days), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddHours(int hours)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddHours(hours)).Single();

			Assert.Equal(_testDate.AddHours(hours), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddMinutes(int minutes)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMinutes(minutes)).Single();

			Assert.Equal(_testDate.AddMinutes(minutes), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddSeconds(int seconds)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddSeconds(seconds)).Single();

			Assert.Equal(_testDate.AddSeconds(seconds), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddMilliseconds(int milliseconds)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMilliseconds(milliseconds)).Single();

			Assert.Equal(_testDate.AddMilliseconds(milliseconds), result);
		}

		[Theory(Skip = "Level of ganularity not supported."), MemberData(nameof(EnumerateValues), -10, 10)]
		public void TestAddTicks(int ticks)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddTicks(ticks)).Single();

			Assert.Equal(_testDate.AddTicks(ticks), result);
		}

		[Theory, MemberData(nameof(EnumerateValues), -10, 10)]
		[Trait("Category", "TimeSpan")]
		public void TestAdd(int millis)
		{
			TimeSpan time = TimeSpan.FromMilliseconds(millis);

			DateTime result = _db.DataTable.Select(d => d.Value.Add(time)).Single();

			Assert.Equal(_testDate.Add(time), result);
		}



		[Fact]
		public void TestYear()
		{
			Assert.Equal(_testDate.Year, _db.DataTable.Select(d => d.Value.Year).Single());
		}

		[Fact]
		public void TestMonth()
		{
			Assert.Equal(_testDate.Month, _db.DataTable.Select(d => d.Value.Month).Single());
		}

		[Fact]
		public void TestDay()
		{
			Assert.Equal(_testDate.Day, _db.DataTable.Select(d => d.Value.Day).Single());
		}

		[Fact]
		public void TestDayOfWeek()
		{
			Assert.Equal(_testDate.DayOfWeek, _db.DataTable.Select(d => d.Value.DayOfWeek).Single());
		}

		[Fact]
		public void TestDayOfYear()
		{
			Assert.Equal(_testDate.DayOfYear, _db.DataTable.Select(d => d.Value.DayOfYear).Single());
		}

		[Fact]
		public void TestHour()
		{
			Assert.Equal(_testDate.Hour, _db.DataTable.Select(d => d.Value.Hour).Single());
		}

		[Fact]
		public void TestMinute()
		{
			Assert.Equal(_testDate.Minute, _db.DataTable.Select(d => d.Value.Minute).Single());
		}

		[Fact]
		public void TestSecond()
		{
			Assert.Equal(_testDate.Second, _db.DataTable.Select(d => d.Value.Second).Single());
		}

		[Fact]
		public void TestMilliSecond()
		{
			Assert.Equal(_testDate.Millisecond, _db.DataTable.Select(d => d.Value.Millisecond).Single());
		}

		[Fact]
		public void TestDate()
		{
			Assert.Equal(_testDate.Date, _db.DataTable.Select(d => d.Value.Date).Single());
		}

		[Fact]
		[Trait("Category", "TimeSpan")]
		public void TestTimeOfDay()
		{
			Assert.Equal(_testDate.TimeOfDay, _db.DataTable.Select(d => d.Value.TimeOfDay).Single());
		}
	}
}
