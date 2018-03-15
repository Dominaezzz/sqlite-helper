using System;
using System.Linq;
using SQLite.Net;
using SQLite.Net.Attributes;
using Xunit;

namespace Tests
{
//	[TestFixture(TestName = "Data Declaration Tests")]
	[Trait("Category", "Create")]
    public class DDLTests
    {
	    public class PlainDb : SQLiteDatabase
	    {
		    public Table<User> Users { get; set; }
			public View<User> UserView { get; set; }
	    }

		[Table("Users"), View("UserView")]
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

		[Fact]
	    public void TestCreateTable()
	    {
		    using (var db = new PlainDb())
		    {
				Assert.Equal(0, db.SQLiteMaster.Count(o => o.Type == "table"));
				
				db.CreateTable<User>(t => new
				{
					UsersUnique = t.Unique(u => new { u.FirstName, u.LastName })
				});

			    Assert.Equal(1, db.SQLiteMaster.Count(o => o.Type == "table"));
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

				Assert.Collection(
					result,
					c =>
					{
						Assert.Equal(0, c.CId);
						Assert.Equal("UserGuid", c.Name);
						Assert.True(c.PrimaryKey);
					},
					c =>
					{
						Assert.Equal(1, c.CId);
						Assert.Equal("FirstName", c.Name);
						Assert.True(c.NotNull);
					},
					c =>
					{
						Assert.Equal(2, c.CId);
						Assert.Equal("LastName", c.Name);
						Assert.True(c.NotNull);
					},
					c =>
					{
						Assert.Equal(3, c.CId);
						Assert.Equal("DateOfBirth", c.Name);
						Assert.False(c.NotNull);
					}
				);

//			    var indexColumns = db.Query("PRAGMA index_info([UsersUnique]);", reader => new
//			    {
//				    No = reader.Get<int>("seqno"),
//				    CId = reader.Get<int>("cid"),
//				    Name = reader.Get<string>("name")
//			    })
//				.ToList();
//
//				Assert.Equal(2, indexColumns.Count);
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

			    Assert.Equal(3, db.SQLiteMaster.Count(o => o.Type == "table"));
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
				
				Assert.Collection(
					result,
					c =>
					{
						Assert.Equal(0, c.CId);
						Assert.Equal("Id", c.Name);
						Assert.True(c.PrimaryKey);
					},
					c =>
					{
						Assert.Equal(1, c.CId);
						Assert.Equal("name", c.Name);
						Assert.True(c.NotNull);
					},
					c =>
					{
						Assert.Equal(2, c.CId);
						Assert.Equal("unit_price", c.Name);
						Assert.True(c.NotNull);
						Assert.Equal(0.0M, Convert.ToDecimal(c.Default));
					},
					c =>
					{
						Assert.Equal(3, c.CId);
						Assert.Equal("expiry_date", c.Name);
						Assert.True(c.NotNull);
					},
					c =>
					{
						Assert.Equal(4, c.CId);
						Assert.Equal("duration", c.Name);
						Assert.True(c.NotNull);
					},
					c =>
					{
						Assert.Equal(5, c.CId);
						Assert.Equal("UserId", c.Name);
						Assert.True(c.NotNull);
					}
				);
			}
	    }

		[Fact]
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
				
				Assert.Collection(
					indexColumns,
					c =>
					{
						Assert.Equal(0, c.No);
						Assert.Equal(1, c.CId);
						Assert.Equal("FirstName", c.Name);
					},
					c =>
					{
						Assert.Equal(1, c.No);
						Assert.Equal(2, c.CId);
						Assert.Equal("LastName", c.Name);
					}
				);
			}
		}

		[Fact]
	    public void TestCreateView()
	    {
			using (var db = new PlainDb())
			{
				db.CreateTable<User>();

				db.CreateView("UserView", db.Users.Where(u => u.DateOfBirth != null));

				Assert.True(db.SQLiteMaster.Any(o => o.Type == "view" && o.Name == "UserView"));
				
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

				Assert.Equal(
					db.Users.Where(u => u.DateOfBirth != null).Select(u => u.UserGuid),
					db.UserView.Select(u => u.UserGuid)
				);
			}
		}

		[Fact(Skip = "Not implemented")]
	    public void TestCreateTrigger()
	    {
		    
	    }
    }
}
