using System;
using System.Linq;
using SQLite.Net;
using SQLite.Net.Attributes;
using SQLite.Net.Exceptions;
using Xunit;

namespace Tests
{
//	[TestFixture(TestName = "Data Manipulation Tests")]
    public class DMLTests : IDisposable
    {
		public class TestDb : SQLiteDatabase
		{
			public Table<Product> Products { get; set; }

			public TestDb()
			{
				if (UserVersion == 0)
				{
					CreateTable("Products", c => new
					{
						Id = c.Column<int>(primaryKey:true),
						Name = c.Column<string>(nullable:false),
						Price = c.Column<decimal>()
					},
					t => new
					{
						UniqueProductNames = t.Unique(p => p.Name)
					});
				}
				UserVersion++;
			}
		}
		[Table("Products")]
		public class Product
		{
			[PrimaryKey]
			public int Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
		}

	    private readonly TestDb _db;

	    public DMLTests()
	    {
		    _db = new TestDb();
	    }

	    public void Dispose()
	    {
		    _db.Dispose();
	    }


		[Fact]
		[Trait("Category", "Insert")]
	    public void TestInsertSimple()
	    {
			Assert.Empty(_db.Products);

		    int count = _db.Products.Insert(new Product { Name = "Laptop", Price = 1500M });
			Assert.Equal(1, count);

			Assert.Collection(_db.Products, product =>
			{
				Assert.Equal(1, product.Id);
				Assert.Equal("Laptop", product.Name);
				Assert.Equal(1500M, product.Price);
			});
	    }

		[Fact]
		[Trait("Category", "Insert")]
		public void TestInsertBatch()
		{
			Assert.Empty(_db.Products);

			var products = new []
			{
				new Product { Name = "Laptop", Price = 1500M },
				new Product { Name = "Computer", Price = 2000M },
				new Product { Name = "Chocolate", Price = 15M },
				new Product { Name = "Piano", Price = 2500M },
				new Product { Name = "Water", Price = 1.5M },
				new Product { Name = "Paper", Price = 0.5M }
			};

			int count = _db.Products.Insert(products);

			Assert.Equal(products.Length, count);

			Assert.Equal(
				products.Select((p, i) => new { Id = i + 1, p.Name, p.Price }),
				_db.Products.Select((p, i) => new { p.Id, p.Name, p.Price })
			);
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertBreaksUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2));
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrIgnoreAgainstUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    count = _db.Products.Insert(product2, Conflict.Ignore);
			Assert.Equal(0, count);
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrReplaceAgainstUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
			Assert.Collection(_db.Products, p =>
			{
				Assert.Equal(1, p.Id);
				Assert.Equal(product1.Name, p.Name);
				Assert.Equal(product1.Price, p.Price);
			});

		    count = _db.Products.Insert(product2, Conflict.Replace);
		    Assert.Equal(1, count);
			Assert.Collection(_db.Products, p =>
			{
				Assert.Equal(2, p.Id);
				Assert.Equal(product2.Name, p.Name);
				Assert.Equal(product2.Price, p.Price);
			});
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrAbortBreaksUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };
			
		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(new[] {product1, product2, product3}, Conflict.Abort));
		    Assert.Empty(_db.Products);
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrFailBreaksUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };

		    int count = _db.Products.Insert(product1);
		    Assert.Equal(1, count);
		    Assert.Equal(1, _db.Products.Count());

		    Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2, Conflict.Fail));
		    Assert.Equal(1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Insert")]
		public void TestInsertOrRollBackBreaksUniqueConstraint()
	    {
		    Assert.Empty(_db.Products);

		    var product1 = new Product { Name = "Laptop", Price = 1500M };
		    var product2 = new Product { Name = "Laptop", Price = 2000M };
		    var product3 = new Product { Name = "Computer", Price = 2300M };

			_db.BeginTransaction();
			{
				Assert.Equal(1, _db.Products.Insert(product1, Conflict.Rollback));
				Assert.Throws<UniqueConstraintException>(() => _db.Products.Insert(product2, Conflict.Rollback));
				Assert.Equal(1, _db.Products.Insert(product3, Conflict.Rollback));
			}
			Assert.Throws<SQLiteException>(() => _db.EndTransaction());
			
			Assert.Collection(_db.Products, p =>
			{
				Assert.Equal(1, p.Id);
				Assert.Equal(product3.Name, p.Name);
				Assert.Equal(product3.Price, p.Price);
			});
	    }


		[Fact]
		[Trait("Category", "Delete")]
	    public void TestDeleteAll()
	    {
			Assert.Empty(_db.Products);

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    _db.Products.Insert(products);
		    Assert.Equal(products.Length, _db.Products.Count());

		    int count = _db.Products.DeleteAll();
			Assert.Equal(products.Length, count);
			Assert.Empty(_db.Products);
	    }

	    [Fact]
	    [Trait("Category", "Delete")]
	    public void TestDelete()
	    {
		    Assert.Empty(_db.Products);

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    int count = _db.Products.Insert(products);

		    Assert.Equal(products.Length, count);
		    Assert.Equal(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Id == 2);

			Assert.Equal(products.Length - 1, _db.Products.Count());
		}

	    [Fact]
	    [Trait("Category", "Delete")]
	    public void TestDeleteMultiple()
	    {
		    Assert.Empty(_db.Products);

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    int count = _db.Products.Insert(products);

		    Assert.Equal(products.Length, count);
		    Assert.Equal(products.Length, _db.Products.Count());

		    _db.Products.Delete(p => p.Price > 1000);

		    Assert.Equal(products.Length - 3, _db.Products.Count());
		}


	    [Fact]
	    [Trait("Category", "Update")]
	    public void TestUpdate()
	    {
		    Assert.Empty(_db.Products);

		    var products = new[]
		    {
			    new Product { Name = "Laptop", Price = 1500M },
			    new Product { Name = "Computer", Price = 2000M },
			    new Product { Name = "Chocolate", Price = 15M },
			    new Product { Name = "Piano", Price = 2500M },
			    new Product { Name = "Water", Price = 1.5M },
			    new Product { Name = "Paper", Price = 0.5M },
		    };

		    Assert.Equal(products.Length, _db.Products.Insert(products));
		    Assert.Equal(products.Length, _db.Products.Count());

			Assert.Contains(_db.Products, p => p.Id == 4 && p.Name == "Piano" && p.Price == 2500);

		    var product = _db.Products.Single(p => p.Id == 4);
		    product.Price = 1234;
			
			Assert.Equal(1, _db.Products.Update(product));

		    Assert.Contains(_db.Products, p => p.Id == 4 && p.Name == "Piano" && p.Price == 1234);
		}
	}
}
