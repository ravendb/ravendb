using System;
using System.Collections.Generic;
using Raven.Client.Documents.Session.TimeSeries;

namespace Orders
{
    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }
        public Contact Contact { get; set; }
        public Address Address { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }

        public class StockPrice
        {
            [TimeSeriesValue(0)] public double Open { get; set; }
            [TimeSeriesValue(1)] public double Close { get; set; }
            [TimeSeriesValue(2)] public double High { get; set; }
            [TimeSeriesValue(3)] public double Low { get; set; }
            [TimeSeriesValue(4)] public double Volume { get; set; }
        }
    }

    public class Address
    {
        public string Line1 { get; set; }
        public string Line2 { get; set; }
        public string City { get; set; }
        public string Region { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
        public Location Location { get; set; }
    }

    public class Location
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public class Contact
    {
        public string Name { get; set; }
        public string Title { get; set; }
    }

    public class Category
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    public class Order
    {
        public string Id { get; set; }
        public string Company { get; set; }
        public string Employee { get; set; }
        public DateTime OrderedAt { get; set; }
        public DateTime RequireAt { get; set; }
        public DateTime? ShippedAt { get; set; }
        public Address ShipTo { get; set; }
        public string ShipVia { get; set; }
        public decimal Freight { get; set; }
        public List<OrderLine> Lines { get; set; }
    }

    public class OrderLine
    {
        public string Product { get; set; }
        public string ProductName { get; set; }
        public decimal PricePerUnit { get; set; }
        public int Quantity { get; set; }
        public decimal Discount { get; set; }
    }

    public class Product
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Supplier { get; set; }
        public string Category { get; set; }
        public string QuantityPerUnit { get; set; }
        public decimal PricePerUnit { get; set; }
        public int UnitsInStock { get; set; }
        public int UnitsOnOrder { get; set; }
        public bool Discontinued { get; set; }
        public int ReorderLevel { get; set; }
    }

    public class Supplier
    {
        public string Id { get; set; }
        public Contact Contact { get; set; }
        public string Name { get; set; }
        public Address Address { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }
        public string HomePage { get; set; }
    }

    public class Employee
    {
        public string Id { get; set; }
        public string LastName { get; set; }
        public string FirstName { get; set; }
        public string Title { get; set; }
        public Address Address { get; set; }
        public DateTime HiredAt { get; set; }
        public DateTime Birthday { get; set; }
        public string HomePhone { get; set; }
        public string Extension { get; set; }
        public string ReportsTo { get; set; }
        public List<string> Notes { get; set; }
        public List<string> Territories { get; set; }

        public class HeartRate
        {
            [TimeSeriesValue(0)] public double BPM { get; set; }
        }
    }

    public class Region
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<Territory> Territories { get; set; }
    }

    public class Territory
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public string Area { get; set; }
    }

    public class Shipper
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Phone { get; set; }
    }
}
