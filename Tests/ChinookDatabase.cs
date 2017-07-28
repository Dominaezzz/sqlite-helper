using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLite.Net;
using SQLite.Net.Attributes;

namespace Tests
{
    public class ChinookDatabase : SQLiteDatabase
    {
		public Table<Album> Albums { get; set; }
		public Table<Artist> Artists { get; set; }
		public Table<Customer> Customers { get; set; }
		public Table<Employee> Employees { get; set; }
		public Table<Genre> Genres { get; set; }
		public Table<Invoice> Invoices { get; set; }
		public Table<InvoiceLine> InvoiceLines { get; set; }
		public Table<MediaType> MediaTypes { get; set; }
		public Table<Playlist> Playlists { get; set; }
		public Table<PlaylistTrack> PlaylistTracks { get; set; }
		public Table<Track> Tracks { get; set; }

	    public ChinookDatabase() : base("chinook.db")
	    {
	    }

	    private void Migrate()
	    {
		    CreateTable("Album", c => new
		    {
			    AlbumId = c.Column<int>(primaryKey:true),
				Title = c.Column<string>(nullable:false),
				ArtistId = c.Column<int>()
		    },
			t => new
			{
				FK = t.ForeignKey(a => a.ArtistId, "Artist", "ArtistId")
			});

			CreateTable("Artist", c => new
			{
				ArtistId = c.Column<int>(primaryKey: true),
				Name = c.Column<string>(nullable: false)
			});

			CreateTable("Customer", c => new
			{
				CustomerId = c.Column<int>(primaryKey:true),
			    FirstName = c.Column<string>(nullable:false),
				LastName = c.Column<string>(nullable: false),
				Company = c.Column<string>(),
				Address = c.Column<string>(),
				City = c.Column<string>(),
				State = c.Column<string>(),
				Country = c.Column<string>(),
				PostalCode = c.Column<string>(),
				Phone = c.Column<string>(),
				Fax = c.Column<string>(),
				Email = c.Column<string>(nullable:false),
				SupportRepId = c.Column<int?>()
			},
			t => new
			{
				FK = t.ForeignKey(c => c.SupportRepId, "Employee", "EmployeeId")
			});

		    CreateTable("Employee", c => new
			{
				EmployeeId = c.Column<int>(primaryKey: true),
				FirstName = c.Column<string>(nullable: false),
				LastName = c.Column<string>(nullable: false),
				Title = c.Column<string>(),
				ReportsTo = c.Column<int>(),
				BirthDate = c.Column<DateTime?>(),
				HireDate = c.Column<DateTime?>(),
				Address = c.Column<string>(),
				City = c.Column<string>(),
				State = c.Column<string>(),
				Country = c.Column<string>(),
				PostalCode = c.Column<string>(),
				Phone = c.Column<string>(),
				Fax = c.Column<string>(),
				Email = c.Column<string>(nullable: false)
			},
			t => new
			{
				FK = t.ForeignKey(c => c.ReportsTo, "Employee", "EmployeeId")
			});

		    CreateTable("Genre", c => new
		    {
			    GenreId = c.Column<int>(primaryKey: true),
			    Name = c.Column<string>()
		    });

		    CreateTable("Invoice", c => new
			{
				InvoiceId = c.Column<int>(primaryKey: true),
				CustomerId = c.Column<int>(),
				InvoiceDate = c.Column<DateTime>(),
				BillingAddress = c.Column<string>(),
				BillingCity = c.Column<string>(),
				BillingState = c.Column<string>(),
				BillingCountry = c.Column<string>(),
				BillingPostalCode = c.Column<string>(),
				Total = c.Column<decimal>()
			},
			t => new
			{
				FK = t.ForeignKey(c => c.CustomerId, "Customer", "CustomerId")
			});

			CreateTable("InvoiceLine", c => new
			{
				InvoiceLineId = c.Column<int>(primaryKey:true),
				InvoiceId = c.Column<int>(),
				TrackId = c.Column<int>(),
				UnitPrice = c.Column<decimal>(),
				Quantity = c.Column<int>()
			},
			t => new
			{
				FK_Invoice = t.ForeignKey(i => i.InvoiceId, "Invoice", "InvoiceId"),
				FK_Track = t.ForeignKey(i => i.TrackId, "Track", "TrackId")
			});

		    CreateTable("MediaType", c => new
		    {
			    MediaTypeId = c.Column<int>(primaryKey: true),
			    Name = c.Column<string>()
		    });

		    CreateTable("Playlist", c => new
		    {
			    PlaylistId = c.Column<int>(primaryKey: true),
			    Name = c.Column<string>()
		    });

		    CreateTable("PlaylistTrack", c => new
		    {
			    PlaylistId = c.Column<int>(),
			    TrackId = c.Column<int>()
		    },
			t => new
			{
				FK_Playlist = t.ForeignKey(pt => pt.PlaylistId, "Playlist", "PlaylistId"),
				FK_Track = t.ForeignKey(pt => pt.TrackId, "Track", "TrackId")
			});

			CreateTable("Track", c => new
			{
				TrackId = c.Column<int>(primaryKey:true),
				Name = c.Column<string>(nullable:false),
				AlbumId = c.Column<int?>(),
				MediaTypeId = c.Column<int>(),
				GenreId = c.Column<int?>(),
				Composer = c.Column<string>(),
				Milliseconds = c.Column<int>(),
				Bytes = c.Column<int?>(),
				UnitPrice = c.Column<decimal>()
			},
			t => new
			{
				FK_Album = t.ForeignKey(c => c.AlbumId, "Album", "AlbumId"),
				FK_MediaType = t.ForeignKey(c => c.MediaTypeId, "MediaType", "MediaTypeId"),
				FK_Genre = t.ForeignKey(c => c.GenreId, "Genre", "GenreId")
			});

			CreateView("AlbumView", Albums.GroupJoin(Tracks, album => album.AlbumId, track => track.AlbumId, (album, tracks) => new
			{
				AlbumId = album.AlbumId,
				Title = album.Title,
				ArtistId = album.ArtistId,
				TotalPrice = tracks.Sum(t => t.UnitPrice),
				TrackCount = tracks.Count()
			}));

			CreateIndex<Track>("Track_AlbumId", false, t => t.AlbumId);

		    Albums.Insert(new Album {ArtistId = 2, Title = "Something"});
	    }
    }

	[Table("Album")]
	public class Album
	{
		[PrimaryKey]
		public int AlbumId { get; set; }

		[NotNull]
		public string Title { get; set; }

		[ForeignKey("Artist", "ArtistId")]
		public int ArtistId { get; set; }
	}

	[Table("Artist")]
	public class Artist
	{
		[PrimaryKey]
		public int ArtistId { get; set; }
		public string Name { get; set; }
	}

	[Table("Customer")]
	public class Customer
	{
		[PrimaryKey]
		public int CustomerId { get; set; }
		[NotNull]
		public string FirstName { get; set; }
		[NotNull]
		public string LastName { get; set; }
		public string Company { get; set; }
		public string Address { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public string Country { get; set; }
		public string PostalCode { get; set; }
		public string Phone { get; set; }
		public string Fax { get; set; }
		[NotNull]
		public string Email { get; set; }

		[ForeignKey("Employee", "EmployeeId")]
		public int? SupportRepId { get; set; }
	}

	[Table("Employee")]
	public class Employee
	{
		[PrimaryKey, NotNull]
		public int EmployeeId { get; set; }
		[NotNull]
		public string LastName { get; set; }
		[NotNull]
		public string FirstName { get; set; }
		public string Title { get; set; }
		[ForeignKey("Employee", "EmployeeId")]
		public int ReportsTo { get; set; }
		public DateTime? BirthDate { get; set; }
		public DateTime? HireDate { get; set; }
		public string Address { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public string Country { get; set; }
		public string PostalCode { get; set; }
		public string Phone { get; set; }
		public string Fax { get; set; }
		[NotNull]
		public string Email { get; set; }
	}

	[Table("Genre")]
	public class Genre
	{
		[PrimaryKey, NotNull]
		public int GenreId { get; set; }
		public string Name { get; set; }
	}

	[Table("Invoice")]
	public class Invoice
	{
		[PrimaryKey]
		public int InvoiceId { get; set; }
		[ForeignKey("Customer", "CustomerId")]
		public int CustomerId { get; set; }
		public DateTime InvoiceDate { get; set; }
		public string BillingAddress { get; set; }
		public string BillingCity { get; set; }
		public string BillingState { get; set; }
		public string BillingCountry { get; set; }
		public string BillingPostalCode { get; set; }
		public decimal Total { get; set; }
	}

	[Table("InvoiceLine")]
	public class InvoiceLine
	{
		[PrimaryKey]
		public int InvoiceLineId { get; set; }
		[ForeignKey("Invoice", "InvoiceId")]
		public int InvoiceId { get; set; }
		[ForeignKey("Track", "TrackId")]
		public int TrackId { get; set; }
		public decimal UnitPrice { get; set; }
		public int Quantity { get; set; }
	}

	[Table("MediaType")]
	public class MediaType
	{
		[PrimaryKey]
		public int MediaTypeId { get; set; }
		public string Name { get; set; }
	}

	[Table("Playlist")]
	public class Playlist
	{
		[PrimaryKey]
		public int PlaylistId { get; set; }
		public string Name { get; set; }
	}

	[Table("PlaylistTrack")]
	public class PlaylistTrack
	{
		[ForeignKey("Playlist", "PlaylistId")]
		public int PlaylistId { get; set; }
		[ForeignKey("Track", "TrackId")]
		public int TrackId { get; set; }
	}

	[Table("Track")]
	public class Track
	{
		[PrimaryKey]
		public int TrackId { get; set; }
		[NotNull]
		public string Name { get; set; }
		[ForeignKey("Album", "AlbumId")]
		public int? AlbumId { get; set; }
		[ForeignKey("MediaType", "MediaTypeId")]
		public int MediaTypeId { get; set; }
		[ForeignKey("Genre", "GenreId")]
		public int? GenreId { get; set; }
		public string Composer { get; set; }
		public int Milliseconds { get; set; }
		public int? Bytes { get; set; }
		public decimal UnitPrice { get; set; }
	}
}
