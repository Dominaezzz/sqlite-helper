using System;
using System.Collections.Generic;
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
    }

	[Table("Album")]
	public class Album
	{
		[PrimaryKey, NotNull]
		public int AlbumId { get; set; }

		[NotNull]
		public string Title { get; set; }

		[ForeignKey("Artist", "ArtistId")]
		public int ArtistId { get; set; }
	}

	[Table("Artist")]
	public class Artist
	{
		[PrimaryKey, NotNull]
		public int ArtistId { get; set; }
		public string Name { get; set; }
	}

	[Table("Customer")]
	public class Customer
	{
		[PrimaryKey, NotNull]
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
		public string ReportsTo { get; set; }
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
		public int Name { get; set; }
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
