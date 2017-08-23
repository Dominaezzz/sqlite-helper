using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
	[TestFixture(TestName = "Data Declaration Tests", Category = "Create")]
    public class DDLTests
    {
	    public class PlainDb : SQLiteDatabase
	    {
		    public Table<User> Users { get; set; }
			public View<UserView> UserView { get; set; }
	    }

		[Table("Users")]
	    public class User
	    {
			[PrimaryKey]
		    public Guid UserGuid { get; set; }
			[NotNull]
			public string FirstName { get; set; }
			[NotNull]
			public string LastName { get; set; }
			public DateTime? DateOfBirth { get; set; }
	    }
		public class UserView
		{
			public Guid UserGuid { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTime? DateOfBirth { get; set; }
		}

		[Test]
	    public void TestCreateTable()
	    {
		    using (var db = new PlainDb())
		    {
				Assert.AreEqual(0, db.SQLiteMaster.Count(o => o.Type == "table"));
				
				db.CreateTable<User>(t => new
				{
					UsersUnique = t.Unique(u => new { u.FirstName, u.LastName })
				});

			    Assert.AreEqual(1, db.SQLiteMaster.Count(o => o.Type == "table"));
				var result = db.Query("PRAGMA table_info([Users]);", reader => new
			    {
				    CId = reader.Get<int>("cid"),
				    Name = reader.Get<string>("name"),
				    Type = reader.Get<string>("type"),
				    NotNull = reader.Get<bool>("notnull"),
				    Default = reader.Get<object>("dflt_value"),
				    PrimaryKey = reader.Get<bool>("pk")
			    })
				.ToList();

				Assert.AreEqual(4, result.Count);
				Assert.IsTrue(result.Any(c => c.CId == 0 && c.Name == "UserGuid" && c.PrimaryKey));
			    Assert.IsTrue(result.Any(c => c.CId == 1 && c.Name == "FirstName" && c.NotNull));
			    Assert.IsTrue(result.Any(c => c.CId == 2 && c.Name == "LastName" && c.NotNull));
			    Assert.IsTrue(result.Any(c => c.CId == 3 && c.Name == "DateOfBirth" && !c.NotNull));

//			    var indexColumns = db.Query("PRAGMA index_info([UsersUnique]);", reader => new
//			    {
//				    No = reader.Get<int>("seqno"),
//				    CId = reader.Get<int>("cid"),
//				    Name = reader.Get<string>("name")
//			    })
//				.ToList();
//
//				Assert.AreEqual(2, indexColumns.Count);
//				Assert.IsTrue(indexColumns.Any(c => c.No == 1 && c.CId == 1 && c.Name == "FirstName"));
//			    Assert.IsTrue(indexColumns.Any(c => c.No == 2 && c.CId == 2 && c.Name == "LastName"));


				db.CreateTable("Test Table", c => new
			    {
				    Id = c.Column<int>(primaryKey:true, autoIncrement:true),
					Name = c.Column<string>("name", nullable:false),
					UnitPrice = c.Column<decimal>("unit_price", defaultValue:0.0M),
					ExpiryDate = c.Column<DateTime>("expiry_date"),
					Duration = c.Column<TimeSpan>("duration"),
					UserId = c.Column<Guid>()
			    },
				t => new
				{
					FK_Users = t.ForeignKey(u => u.UserId, "Users", "UserGuid")
				});

			    Assert.AreEqual(3, db.SQLiteMaster.Count(o => o.Type == "table"));
				result = db.Query("PRAGMA table_info([Test Table]);", reader => new
				    {
					    CId = reader.Get<int>("cid"),
					    Name = reader.Get<string>("name"),
					    Type = reader.Get<string>("type"),
					    NotNull = reader.Get<bool>("notnull"),
					    Default = reader.Get<object>("dflt_value"),
					    PrimaryKey = reader.Get<bool>("pk")
				    })
				    .ToList();

			    Assert.AreEqual(6, result.Count);
			    Assert.IsTrue(result.Any(c => c.CId == 0 && c.Name == "Id" && c.PrimaryKey));
			    Assert.IsTrue(result.Any(c => c.CId == 1 && c.Name == "name" && c.NotNull));
			    Assert.IsTrue(result.Any(c => c.CId == 2 && c.Name == "unit_price" && c.NotNull && Convert.ToDecimal(c.Default) == 0.0M));
			    Assert.IsTrue(result.Any(c => c.CId == 3 && c.Name == "expiry_date" && c.NotNull));
			    Assert.IsTrue(result.Any(c => c.CId == 4 && c.Name == "duration" && c.NotNull));
			    Assert.IsTrue(result.Any(c => c.CId == 5 && c.Name == "UserId" && c.NotNull));
			}
	    }

		[Test]
	    public void TestCreateIndex()
	    {
		    using (var db = new PlainDb())
		    {
				db.CreateTable<User>();
				db.CreateIndex<User>("UsersUnique", true, u => new { u.FirstName , u.LastName });

			    var indexColumns = db.Query("PRAGMA index_info([UsersUnique]);", reader => new
			    {
				    No = reader.Get<int>("seqno"),
				    CId = reader.Get<int>("cid"),
				    Name = reader.Get<string>("name")
			    })
				.ToList();

				Assert.AreEqual(2, indexColumns.Count);
				Assert.IsTrue(indexColumns.Any(c => c.No == 0 && c.CId == 1 && c.Name == "FirstName"));
			    Assert.IsTrue(indexColumns.Any(c => c.No == 1 && c.CId == 2 && c.Name == "LastName"));
			}
		}

		[Test]
	    public void TestCreateView()
	    {
			using (var db = new PlainDb())
			{
				db.CreateTable<User>();

				db.CreateView("UserView", db.Users.Where(u => u.DateOfBirth != null));

				Assert.IsTrue(db.SQLiteMaster.Any(o => o.Type == "view" && o.Name == "UserView"));
				
				db.Users.Insert(new[]
				{
					new User { UserGuid = Guid.NewGuid(), FirstName = "AB", LastName = "CD" },
					new User { UserGuid = Guid.NewGuid(), FirstName = "EF", LastName = "GH" },
					new User { UserGuid = Guid.NewGuid(), FirstName = "IJ", LastName = "KL", DateOfBirth = DateTime.Now},
					new User { UserGuid = Guid.NewGuid(), FirstName = "MN", LastName = "OP" },
					new User { UserGuid = Guid.NewGuid(), FirstName = "QR", LastName = "ST" , DateOfBirth = DateTime.Now},
					new User { UserGuid = Guid.NewGuid(), FirstName = "UV", LastName = "WX" },
					new User { UserGuid = Guid.NewGuid(), FirstName = "YZ", LastName = "AZ", DateOfBirth = DateTime.Now},
					new User { UserGuid = Guid.NewGuid(), FirstName = "BY", LastName = "CX" }
				});

				Assert.AreEqual(db.Users.Count(u => u.DateOfBirth != null), db.UserView.Count());
			}
		}

		[Test]
		[NUnit.Framework.Ignore("Not implemented")]
	    public void TestCreateTrigger()
	    {
		    
	    }
    }
}
