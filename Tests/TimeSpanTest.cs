using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Tests
{
	[TestFixture(Category = "TimeSpan")]
	public class TimeSpanTest
	{
		private SQLiteDb<TimeSpan> _db;
		private readonly TimeSpan _testTimeSpan = new TimeSpan(10, 2, 30, 4, 567).Add(TimeSpan.FromTicks(100));
		private readonly TimeSpan _testTimeSpan2 = new TimeSpan(5, 6, 20, 9, 713).Add(TimeSpan.FromTicks(300));

		[OneTimeSetUp]
		public void TestSetUp()
		{
			_db = new SQLiteDb<TimeSpan>();
			_db.DataTable.Insert(new Data<TimeSpan> { Value = _testTimeSpan });

			_db.Logger = Console.Out;
		}

		[OneTimeTearDown]
		public void TestTearDown()
		{
			_db.Dispose();
		}


		[Test]
		public void TestTotalDays()
		{
			Assert.AreEqual(_testTimeSpan.TotalDays, _db.DataTable.Select(d => d.Value.TotalDays).Single());
		}

		[Test]
		public void TestTotalHours()
		{
			Assert.AreEqual(_testTimeSpan.TotalHours, _db.DataTable.Select(d => d.Value.TotalHours).Single(), 0.0000000000001);
		}

		[Test]
		public void TestTotalMinutes()
		{
			Assert.AreEqual(_testTimeSpan.TotalMinutes, _db.DataTable.Select(d => d.Value.TotalMinutes).Single());
		}

		[Test]
		public void TestTotalSeconds()
		{
			Assert.AreEqual(_testTimeSpan.TotalSeconds, _db.DataTable.Select(d => d.Value.TotalSeconds).Single(), 0.0001);
		}

		[Test]
		public void TestTotalMilliSeconds()
		{
			Assert.AreEqual(_testTimeSpan.TotalMilliseconds, _db.DataTable.Select(d => d.Value.TotalMilliseconds).Single());
		}


		[Test]
		public void TestDays()
		{
			Assert.AreEqual(_testTimeSpan.Days, _db.DataTable.Select(d => d.Value.Days).Single());
		}

		[Test]
		public void TestHours()
		{
			Assert.AreEqual(_testTimeSpan.Hours, _db.DataTable.Select(d => d.Value.Hours).Single());
		}

		[Test]
		public void TestMinutes()
		{
			Assert.AreEqual(_testTimeSpan.Minutes, _db.DataTable.Select(d => d.Value.Minutes).Single());
		}

		[Test]
		public void TestSeconds()
		{
			Assert.AreEqual(_testTimeSpan.Seconds, _db.DataTable.Select(d => d.Value.Seconds).Single());
		}

		[Test]
		public void TestMilliSeconds()
		{
			Assert.AreEqual(_testTimeSpan.Milliseconds, _db.DataTable.Select(d => d.Value.Milliseconds).Single());
		}

		[Test]
		public void TestTicks()
		{
			Assert.AreEqual(_testTimeSpan.Ticks, _db.DataTable.Select(d => d.Value.Ticks).Single());
		}


		[Test]
		[TestCase(13.034)]
		public void TestFromDays(double value)
		{
			Assert.AreEqual(
				TimeSpan.FromDays(value), _db.DataTable.Select(d => TimeSpan.FromDays(d.Id * value)).Single()
			);
		}

		[Test]
		[TestCase(13.034)]
		public void TestFromHours(double value)
		{
			Assert.AreEqual(
				TimeSpan.FromHours(value), _db.DataTable.Select(d => TimeSpan.FromHours(d.Id * value)).Single()
			);
		}

		[Test]
		[TestCase(13.034)]
		public void TestFromMinutes(double value)
		{
			Assert.AreEqual(
				TimeSpan.FromMinutes(value), _db.DataTable.Select(d => TimeSpan.FromMinutes(d.Id * value)).Single()
			);
		}

		[Test]
		[TestCase(13.034)]
		public void TestFromSeconds(double value)
		{
			Assert.AreEqual(
				TimeSpan.FromSeconds(value), _db.DataTable.Select(d => TimeSpan.FromSeconds(d.Id * value)).Single()
			);
		}

		[Test]
		[TestCase(13.0)]
		public void TestFromMilliSeconds(double value)
		{
			Assert.AreEqual(
				TimeSpan.FromMilliseconds(value),
				_db.DataTable.Select(d => TimeSpan.FromMilliseconds(d.Id * value)).Single()
			);
		}

		[Test]
		[TestCase(123456789)]
		public void TestFromTicks(long value)
		{
			Assert.AreEqual(
				TimeSpan.FromTicks(value),
				_db.DataTable.Select(d => TimeSpan.FromTicks(d.Id * value)).Single()
			);
		}


		[Test]
		public void TestAdd()
		{
			Assert.AreEqual(
				_testTimeSpan.Add(_testTimeSpan2),
				_db.DataTable.Select(d => d.Value.Add(_testTimeSpan2)).Single()
			);
		}

		[Test]
		public void TestSubtract()
		{
			Assert.AreEqual(
				_testTimeSpan.Subtract(_testTimeSpan2),
				_db.DataTable.Select(d => d.Value.Subtract(_testTimeSpan2)).Single()
			);
		}

		[Test]
		public void TestNegate()
		{
			Assert.AreEqual(_testTimeSpan.Negate(), _db.DataTable.Select(d => d.Value.Negate()).Single());
		}

		[Test]
		public void TestDuration()
		{
			Assert.AreEqual(_testTimeSpan.Duration(), _db.DataTable.Select(d => d.Value.Duration()).Single());
		}
	}
}
