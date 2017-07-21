# SQLite-helper
An SQLite ORM and client for .NET applications.

SQLite-helper is an open source library built using .NETStandard to be cross-platform.

The main goal of the library was to support complex LINQ queries.

Install [SQLite Helper](https://www.nuget.org/packages/sqlite-helper/) from Nuget.

# Examples
Classes can be defined to access tables or views in the database.
Optionally they can be decorated with attributes to allow the library make smarter when working with the database.
You don't need to define classes to access data but it makes it a whole lot easier.

```csharp
[Table("Products")]
public class Product
{
    [PrimaryKey]
    public int Id { get; set; }
    public string Name { get; set; }
    public decimal Price { get; set; }
}

[Table("Purchases")]
public class Purchase
{
    [PrimaryKey]
    public int Id { get; set}
    public string Customer { get; set; }
    public DateTime Date { get; set; }
    [ForeignKey("Products", "Id")]
    public int ProductId { get; set; }
}

public class ProductView
{
    public int Id { get; set; }
    public string Name { get; set; }
    public decimal Price { get; set; }
    public int PurchaseCount { get; set; }
}
```

To use the models you extend the SQLiteDatabase class.
If you wish to create tables and/or views there are methods to help.
You can even create views from LINQ queries!

```csharp
public class SQLiteDb : SQLiteDatabase
{
    public Table<Product> Products { get; set; }
    public Table<Purchase> Purchases { get; set; }
    public View<ProductView> ProductView { get; set; }

    public SQLiteDb() : base(/** Optional path to file. **/)
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

            CreateTable("Purchases", c => new
            {
                Id = c.Column<int>(primaryKey:true),
                Customer = c.Column<string>(nullable:false),
                Date = c.Column<DateTime>(),
                ProductId = c.Column<int>()
            },
            t => new
            {
                FK = t.ForeignKey(p => p.ProductId, "Products", new[]{ "Id" })
            });

            CreateView(
                "ProductView",
                Products.GroupJoin(Purchases, p => p.Id, p => p.ProductId, (product, purchases) => new ProductView
                {
                    Id = product.Id,
                    Name = product.Name,
                    Price = product.Price,
                    PurchaseCount = purchases.Count()
                })
            );
        }
        UserVersion++;
    }
}
```

You can insert data into tables like so, using the insert method of tables.

```csharp
using(var db = new SQLiteDb())
{
    db.Products.Insert(new Product { Name = "Laptop", Price = 1499.99M });
}
```

You can query the database easily using the IQueryable methods or LINQ.

Most methods are supported for querying, including Where, OrderBy, Join, GroupBy, Select, SelectMany, etc.

NOTE: The database is not actually touched until enumeration happens, like when used in a foreach loop or any of the methods that do not return an IQueryable<...> are called e.g `ToList`, `ToArray`, `Single`, `First`, `Any` etc.

```csharp
using(var db = new SQLiteDb())
{
    var result = db.Products.Where(p => p.Name.Length < 10 && p.Price > 19.99);
    for (var product in result)
    {
        Console.WriteLine($"Name={product.Name}, Price={product.Price}");
    }
}
```

I don't know why but...  If you decide LINQ is not for you and you would rather type raw SQL.
There is the option of using the query method and specifying your own custom projector.

```csharp
using(var db = new SQLiteDb())
{
    var result = db.Query("SELECT * FROM [Products]", r => new
    {
         Name = r.Get<string>("Name"),
         Price = r.Get<decimal>("Price")
    });
    for (var product in result)
    {
        Console.WriteLine($"Item={item}");
    }
}
```

If you have a good reason for typing raw sql such as, a query that cannot be easily done with LINQ.
But still want to integrate some LINQ after, you still can.
And the best part is that, it is still sent over to the database so it doesn't execute on the client side.

```csharp
using (var db = new SQLiteDb())
{
    var result = db.Query("SELECT * FROM [Products]", r => new
    {
         Name = r.Get<string>("Name"),
         Price = r.Get<decimal>("Price")
    })
    .Where(p => p.Name.Length < 10 && p.Price > 19.99);

    for (var product in result)
    {
        Console.WriteLine($"Item={item}");
    }
}
```
