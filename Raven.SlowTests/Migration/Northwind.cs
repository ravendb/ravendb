// -----------------------------------------------------------------------
//  <copyright file="Northwind.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.SlowTests.Migration
{
	namespace Orders
	{
		public class ProductSales : AbstractIndexCreationTask
		{
			public class Result
			{
				public string Product { get; set; }

				public int Count { get; set; }

				public double Total { get; set; }
			}

			public override string IndexName
			{
				get
				{
					return "Product/Sales";
				}
			}
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"from order in docs.Orders
from line in order.Lines
select new { Product = line.Product, Count = 1, Total = ((line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount)) }",
					Reduce = @"from result in results
group result by result.Product into g
select new
{
	Product = g.Key,
	Count = g.Sum(x=>x.Count),
	Total = g.Sum(x=>x.Total)
}",
					MaxIndexOutputsPerDocument = 30
				};
			}
		}

		public class OrdersTotals : AbstractIndexCreationTask
		{
			public class Result
			{
				public string Employee { get; set; }

				public string Company { get; set; }

				public double Total { get; set; }
			}

			public override string IndexName
			{
				get
				{
					return "Orders/Totals";
				}
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"from order in docs.Orders
select new { order.Employee,  order.Company, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }",
					SortOptions = { { "Total", SortOptions.Double } }
				};
			}
		}

		public class OrdersByCompany : AbstractIndexCreationTask
		{
			public class Result
			{
				public string Company { get; set; }

				public int Count { get; set; }

				public double Total { get; set; }
			}

			public override string IndexName
			{
				get
				{
					return "Orders/ByCompany";
				}
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"from order in docs.Orders
select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }",
					Reduce = @"from result in results
group result by result.Company into g
select new
{
	Company = g.Key,
	Count = g.Sum(x=>x.Count),
	Total = g.Sum(x=>x.Total)
}"
				};
			}
		}

		public class Company
		{
			public string Id { get; set; }
			public string ExternalId { get; set; }
			public string Name { get; set; }
			public Contact Contact { get; set; }
			public Address Address { get; set; }
			public string Phone { get; set; }
			public string Fax { get; set; }
		}

		public class Address
		{
			public string Line1 { get; set; }
			public string Line2 { get; set; }
			public string City { get; set; }
			public string Region { get; set; }
			public string PostalCode { get; set; }
			public string Country { get; set; }
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
		}

		public class Shipper
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Phone { get; set; }
		}
	}
}