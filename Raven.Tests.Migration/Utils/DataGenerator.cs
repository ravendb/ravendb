// -----------------------------------------------------------------------
//  <copyright file="DataGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using FakeData;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Migration.Indexes;
using Raven.Tests.Migration.Utils.Orders;

namespace Raven.Tests.Migration.Utils
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

	public class DataGenerator
	{
		private const int NumberOfOrdersPerIteration = 500;

		private const int NumberOfCompaniesPerIteration = 1;

		private const int NumberOfEmployeesPerIteration = 1;

		private const int NumberOfProductsPerIteration = 10;

		private readonly ThinClient client;

		private readonly int numberOfIterations;

		public DataGenerator(ThinClient client, int numberOfIterations)
		{
			this.client = client;
			this.numberOfIterations = numberOfIterations;
		}

		public int ExpectedNumberOfOrders
		{
			get
			{
				return numberOfIterations * NumberOfOrdersPerIteration;
			}
		}

		public int ExpectedNumberOfCompanies
		{
			get
			{
				return numberOfIterations * NumberOfCompaniesPerIteration;
			}
		}

		public int ExpectedNumberOfProducts
		{
			get
			{
				return numberOfIterations * NumberOfProductsPerIteration;
			}
		}

		public int ExpectedNumberOfEmployees
		{
			get
			{
				return numberOfIterations * NumberOfEmployeesPerIteration;
			}
		}

		public int ExpectedNumberOfIndexes
		{
			get
			{
				return 6;
			}
		}

		public void Generate()
		{
			var random = new Random();

			client.PutIndex(new RavenDocumentsByEntityName());
			client.PutIndex(new OrdersByCompany());
			client.PutIndex(new OrdersTotals());
			client.PutIndex(new ProductSales());
			client.PutIndex(new OrdersByEmployeeAndCompany());
			client.PutIndex(new OrdersByEmployeeAndCompanyReduce());

			var nextOrderId = 1;
			var nextCompanyId = 1;
			var nextProductId = 1;
			var nextEmployeeId = 1;

			for (var it = 0; it < numberOfIterations; it++)
			{
				var entities = new List<object>();

				for (var i = 0; i < NumberOfOrdersPerIteration; i++)
				{
					entities.Add(new Order
					{
						Id = "orders/" + nextOrderId++,
						Company = "companies/" + random.Next(1, ExpectedNumberOfCompanies - 1),
						Lines = CollectionData.GetElement(10, new List<OrderLine>
							{
								new OrderLine
								{
									Product = "products/" + random.Next(1, ExpectedNumberOfProducts - 1),
									Quantity = random.Next(1, 100),
									PricePerUnit = random.Next(1000)
								}
							}).ToList(),
						Employee = "employees/" + random.Next(1, ExpectedNumberOfEmployees - 1),
						OrderedAt = DateTimeData.GetDatetime(),
						RequireAt = DateTime.UtcNow.AddDays(7),
						ShipTo = new Address
						{
							Country = PlaceData.GetCountry(),
							City = PlaceData.GetCity(),
							PostalCode = PlaceData.GetZipCode(),
							Line1 = PlaceData.GetStreetName()
						}
					});
				}

				for (var i = 0; i < NumberOfCompaniesPerIteration; i++)
				{
					entities.Add(new Company
					{
						Id = "companies/" + nextCompanyId++,
						Name = NameData.GetCompanyName(),
						Fax = PhoneNumberData.GetInternationalPhoneNumber(),
						Address = new Address
						{
							Country = PlaceData.GetCountry(),
							City = PlaceData.GetCity(),
							PostalCode = PlaceData.GetZipCode(),
							Line1 = PlaceData.GetStreetName()
						}
					});
				}

				for (var i = 0; i < NumberOfEmployeesPerIteration; i++)
				{
					entities.Add(new Employee
					{
						Id = "employees/" + nextEmployeeId++,
						Birthday = DateTimeData.GetDatetime(),
						FirstName = NameData.GetFirstName(),
						LastName = NameData.GetSurname(),
						HomePhone = PhoneNumberData.GetPhoneNumber(),
						HiredAt = DateTimeData.GetDatetime(),
						Address = new Address
						{
							Country = PlaceData.GetCountry(),
							City = PlaceData.GetCity(),
							PostalCode = PlaceData.GetZipCode(),
							Line1 = PlaceData.GetStreetName()
						}
					});
				}

				for (var i = 0; i < NumberOfProductsPerIteration; i++)
				{
					entities.Add(new Product
					{
						Id = "products/" + nextProductId++,
						Category = TextData.GetAlphabetical(5),
						Name = NameData.GetSurname(),
						Discontinued = BooleanData.GetBoolean(),
						PricePerUnit = NumberData.GetNumber()
					});
				}

				client.PutEntities(entities);
			}
		}
	}
}