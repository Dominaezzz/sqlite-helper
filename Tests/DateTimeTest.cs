using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace Tests
{
	[TestFixture(Category = "DateTime")]
    public class DateTimeTest
	{
		private SQLiteDb<DateTime> _db;
		private readonly DateTime _testDate = new DateTime(2017, 7, 13, 9, 55, 18, 123);
		
		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new SQLiteDb<DateTime>();
			_db.DataTable.Insert(new Data<DateTime> { Value = _testDate });

			_db.Log = Console.WriteLine;
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}

		public static IEnumerable<int> EnumerateValues(int from, int to)
		{
			return Enumerable.Range(from, to - from);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[]{ -10, 10 })]
		public void TestAddYears(int years)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddYears(years)).Single();

			Assert.AreEqual(_testDate.AddYears(years), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddMonths(int months)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMonths(months)).Single();

			Assert.AreEqual(_testDate.AddMonths(months), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddDays(int days)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddDays(days)).Single();

			Assert.AreEqual(_testDate.AddDays(days), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddHours(int hours)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddHours(hours)).Single();

			Assert.AreEqual(_testDate.AddHours(hours), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddMinutes(int minutes)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMinutes(minutes)).Single();

			Assert.AreEqual(_testDate.AddMinutes(minutes), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddSeconds(int seconds)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddSeconds(seconds)).Single();

			Assert.AreEqual(_testDate.AddSeconds(seconds), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAddMilliseconds(int milliseconds)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddMilliseconds(milliseconds)).Single();

			Assert.AreEqual(_testDate.AddMilliseconds(milliseconds), result);
		}

		[Test]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		[Ignore("Level of ganularity not supported.")]
		public void TestAddTicks(int ticks)
		{
			DateTime result = _db.DataTable.Select(d => d.Value.AddTicks(ticks)).Single();

			Assert.AreEqual(_testDate.AddTicks(ticks), result);
		}

		[Test]
		[Category("TimeSpan")]
		[TestCaseSource(nameof(EnumerateValues), new object[] { -10, 10 })]
		public void TestAdd(int millis)
		{
			TimeSpan time = TimeSpan.FromMilliseconds(millis);

			DateTime result = _db.DataTable.Select(d => d.Value.Add(time)).Single();

			Assert.AreEqual(_testDate.Add(time), result);
		}



		[Test]
		public void TestYear()
		{
			Assert.AreEqual(_testDate.Year, _db.DataTable.Select(d => d.Value.Year).Single());
		}

		[Test]
		public void TestMonth()
		{
			Assert.AreEqual(_testDate.Month, _db.DataTable.Select(d => d.Value.Month).Single());
		}

		[Test]
		public void TestDay()
		{
			Assert.AreEqual(_testDate.Day, _db.DataTable.Select(d => d.Value.Day).Single());
		}

		[Test]
		public void TestDayOfWeek()
		{
			Assert.AreEqual(_testDate.DayOfWeek, _db.DataTable.Select(d => d.Value.DayOfWeek).Single());
		}

		[Test]
		public void TestDayOfYear()
		{
			Assert.AreEqual(_testDate.DayOfYear, _db.DataTable.Select(d => d.Value.DayOfYear).Single());
		}

		[Test]
		public void TestHour()
		{
			Assert.AreEqual(_testDate.Hour, _db.DataTable.Select(d => d.Value.Hour).Single());
		}

		[Test]
		public void TestMinute()
		{
			Assert.AreEqual(_testDate.Minute, _db.DataTable.Select(d => d.Value.Minute).Single());
		}

		[Test]
		public void TestSecond()
		{
			Assert.AreEqual(_testDate.Second, _db.DataTable.Select(d => d.Value.Second).Single());
		}

		[Test]
		public void TestMilliSecond()
		{
			Assert.AreEqual(_testDate.Millisecond, _db.DataTable.Select(d => d.Value.Millisecond).Single());
		}

		[Test]
		public void TestDate()
		{
			Assert.AreEqual(_testDate.Date, _db.DataTable.Select(d => d.Value.Date).Single());
		}

		[Test]
		[Category("TimeSpan")]
		public void TestTimeOfDay()
		{
			Assert.AreEqual(_testDate.TimeOfDay, _db.DataTable.Select(d => d.Value.TimeOfDay).Single());
		}
	}
}
