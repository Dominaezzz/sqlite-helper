using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[TestFixture]
    public class ConsistencyTest
    {
	    private static void SimpleTest<T>(T value)
	    {
			using (var db = new SQLiteDb<T>())
			{
				db.DataTable.Insert(new Data<T> { Value = value });

				Assert.AreEqual(value, db.DataTable.Single().Value);
			}
		}

	    [Test]
	    public void TestBool()
	    {
		    SimpleTest<bool>(true);
		    SimpleTest<bool>(false);
		    SimpleTest<bool?>(true);
		    SimpleTest<bool?>(false);
		    SimpleTest<bool?>(null);
		}

	    [Test]
	    public void TestChar()
	    {
		    SimpleTest<char>('A');
		    SimpleTest<char?>('A');
		    SimpleTest<char?>(null);
	    }

		[Test]
	    public void TestByte()
	    {
		    SimpleTest<byte>(10);
		    SimpleTest<byte?>(10);
		    SimpleTest<byte?>(null);
		}

	    [Test]
	    public void TestSByte()
	    {
		    SimpleTest<sbyte>(10);
		    SimpleTest<sbyte?>(10);
		    SimpleTest<sbyte?>(null);
		}

	    [Test]
	    public void TestInt16()
	    {
			SimpleTest<short>(10);
		    SimpleTest<short?>(10);
		    SimpleTest<short?>(null);
		}

	    [Test]
	    public void TestUInt16()
	    {
		    SimpleTest<ushort>(10);
		    SimpleTest<ushort?>(10);
		    SimpleTest<ushort?>(null);
		}

	    [Test]
	    public void TestInt32()
	    {
		    SimpleTest<int>(10);
		    SimpleTest<int?>(10);
		    SimpleTest<int?>(null);
		}

	    [Test]
	    public void TestUInt32()
	    {
		    SimpleTest<uint>(10);
		    SimpleTest<uint?>(10);
		    SimpleTest<uint?>(null);
		}

	    [Test]
	    public void TestInt64()
	    {
		    SimpleTest<long>(10);
		    SimpleTest<long?>(10);
		    SimpleTest<long?>(null);
		}

	    [Test]
	    public void TestUInt64()
	    {
		    SimpleTest<ulong>(10);
		    SimpleTest<ulong?>(10);
		    SimpleTest<ulong?>(null);
		}

	    [Test]
	    public void TestFloat()
	    {
		    SimpleTest<float>(45.783F);
		    SimpleTest<float?>(45.783F);
		    SimpleTest<float?>(null);
		}

	    [Test]
	    public void TestDouble()
	    {
		    SimpleTest<double>(16.5532134D);
		    SimpleTest<double?>(16.5532134D);
		    SimpleTest<double?>(null);
		}

	    [Test]
	    public void TestDecimal()
	    {
		    SimpleTest<decimal>(123.456789M);
		    SimpleTest<decimal?>(123.456789M);
		    SimpleTest<decimal?>(null);
		}

	    [Test]
	    public void TestByteArray()
	    {
		    SimpleTest<byte[]>(new byte[] { 0, 12, 45, 2, 78 });
		    SimpleTest<byte[]>(new byte[] { 0 });
		    SimpleTest<byte[]>(new byte[] { });
		    SimpleTest<byte[]>(null);
		}

	    [Test]
		[Category("Strings")]
	    public void TestString()
	    {
		    SimpleTest<string>("Test String");
		    SimpleTest<string>("");
		    SimpleTest<string>(null);
		}

	    [Test]
		[Category("TimeSpan")]
	    public void TestTimeSpan()
	    {
		    SimpleTest<TimeSpan>(TimeSpan.FromMilliseconds(123459123));
		    SimpleTest<TimeSpan?>(TimeSpan.FromMilliseconds(1289123));
		    SimpleTest<TimeSpan?>(null);
		}

	    [Test]
		[Category("DateTime")]
	    public void TestDateTime()
	    {
			DateTime now = DateTime.Now;
		    DateTime date = now - TimeSpan.FromTicks(now.Ticks % 10000); // Rounds up to nearest millisecond.

			SimpleTest<DateTime>(date);
		    SimpleTest<DateTime?>(date);
		    SimpleTest<DateTime?>(null);
		}

	    [Test]
	    public void TestDateTimeOffset()
		{
			DateTimeOffset now = DateTimeOffset.Now;
			DateTimeOffset date = now - TimeSpan.FromTicks(now.Ticks % 10000); // Rounds up to nearest millisecond.

			SimpleTest<DateTimeOffset>(date);
			SimpleTest<DateTimeOffset?>(date);
			SimpleTest<DateTimeOffset?>(null);
		}

	    [Test]
	    public void TestGuid()
	    {
		    SimpleTest<Guid>(Guid.NewGuid());
		    SimpleTest<Guid?>(Guid.NewGuid());
		    SimpleTest<Guid?>(null);
		}

	    [Test]
	    public void TestEnum()
	    {
		    SimpleTest<RegularEnum>(RegularEnum.Value1);
		    SimpleTest<RegularEnum>(RegularEnum.Value2);
		    SimpleTest<RegularEnum>(RegularEnum.Value3);
		    SimpleTest<RegularEnum?>(RegularEnum.Value3);
		    SimpleTest<RegularEnum?>(null);

		    SimpleTest<IrregularEnum>(IrregularEnum.Value1);
		    SimpleTest<IrregularEnum>(IrregularEnum.Value2);
		    SimpleTest<IrregularEnum>(IrregularEnum.Value3);
		    SimpleTest<IrregularEnum?>(IrregularEnum.Value3);
		    SimpleTest<IrregularEnum?>(null);

		    SimpleTest<TextEnum>(TextEnum.Table);
		    SimpleTest<TextEnum>(TextEnum.View);
		    SimpleTest<TextEnum>(TextEnum.Trigger);
		    SimpleTest<TextEnum>(TextEnum.Index);
			SimpleTest<TextEnum?>(TextEnum.View);
		    SimpleTest<TextEnum?>(null);
		}

	    public enum RegularEnum
	    {
		    Value1, Value2, Value3
		}

	    public enum IrregularEnum
	    {
		    Value1 = 3, Value2 = 1, Value3 = 9
	    }

		[StoreAsText]
	    public enum TextEnum
	    {
		    Table, View, Trigger, Index
	    }
	}
}
