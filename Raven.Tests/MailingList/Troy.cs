// -----------------------------------------------------------------------
//  <copyright file="Troy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Troy : RavenTest
	{
		[Fact]
		public void QueryProductWithPhraseOrOtherTermsByDepartmentAndSwitchTermOrder()
		{
			using (var store = CreateStore())
			using (var session = store.OpenSession())
			{

				//Create the products
				var products = CreateProducts();
				products.ForEach(session.Store);
				session.SaveChanges();

				// *****************************************************************************************************************************************
				// Confirm our products are in Raven
				// *****************************************************************************************************************************************
				var searchResults = session.Query<Product>().Customize(x => x.WaitForNonStaleResults());
				Assert.True(searchResults.Count() == 5);

				RavenQueryStatistics stats;

				// *****************************************************************************************************************************************
				// We fail to find any Products as expected - Note Phrase is not a match
				// *****************************************************************************************************************************************
				var results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .Where("\"Gigabit Switch Network\"")
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(!results.Any());

				// *****************************************************************************************************************************************
				// We find 1 Product - Note Phrase is not a match, it matches on the word "Vertical" in the Attributes
				// *****************************************************************************************************************************************
				results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .OpenSubclause()
				  .Where("\"Gigabit Switch Network\" Vertical")
				  .CloseSubclause()
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(results.Count<Product>() == 1);

				// *****************************************************************************************************************************************
				// We SHOULD find 1 Product - Note Phrase is not a match, it SHOULD match on the word "Vertical" in the Attributes
				// *****************************************************************************************************************************************
				results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .OpenSubclause()
				  .Where("Vertical \"Gigabit Switch Network\"") // <-- Only difference from above successful test, is putting the single term in front of phrase
				  .CloseSubclause()
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(results.Count<Product>() == 1);
			}
		}

		[Fact]
		public void QueryProductWithPhraseOrOtherTermsByDepartmentSeemsToBeAndSearch()
		{
			using (var store = CreateStore())
			using (var session = store.OpenSession())
			{

				//Create the products
				var products = CreateProducts();
				products.ForEach(session.Store);
				session.SaveChanges();

				// *****************************************************************************************************************************************
				// Confirm our products are in Raven
				// *****************************************************************************************************************************************
				var searchResults = session.Query<Product>().Customize(x => x.WaitForNonStaleResults());
				Assert.True(searchResults.Count() == 5);

				RavenQueryStatistics stats;

				// *****************************************************************************************************************************************
				// We fail to find any Products as expected - Note Phrase is not a match
				// *****************************************************************************************************************************************
				var results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .Where("\"Gigabit Switch Network\"")
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(!results.Any());

				// *****************************************************************************************************************************************
				// We find 2 Products - Note Phrase is not a match, it matches on the word "Switch"
				// *****************************************************************************************************************************************
				results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .OpenSubclause()
				  .Where("\"Gigabit Switch Network\" Switch")
				  .CloseSubclause()
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(results.Count<Product>() == 2);

				// *****************************************************************************************************************************************
				// We find 2 Products - Note Phrase is not a match, it matches on the word "Sound" in the attributes
				// *****************************************************************************************************************************************
				results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .OpenSubclause()
				  .Where("\"Gigabit Switch Network\" Sound")
				  .CloseSubclause()
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(results.Count<Product>() == 1);

				// *****************************************************************************************************************************************
				// We SHOULD find 3 Products - Note Phrase is not a match, it should match on the words "Switch" in Name or "Sound" in the attributes
				// *****************************************************************************************************************************************
				results = session.Advanced.LuceneQuery<Product, Product_Search>()
				  .UsingDefaultField("Query")
				  .OpenSubclause()
				  .Where("\"Gigabit Switch Network\" Switch Sound") // <- This should be "Gigabit Switch Network" OR Switch OR Sound
				  .CloseSubclause()
				  .AndAlso()
				  .WhereEquals("Department", "Electronics")
				  .WaitForNonStaleResults()
				  .Statistics(out stats);
				Assert.True(results.Count<Product>() == 3);
			}
		}

		public class Product
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
			public string Category { get; set; }
			public string Department { get; set; }
			public List<string> Attributes { get; set; }
			public DateTimeOffset Created { get; set; }
		}

		private static EmbeddableDocumentStore CreateStore()
		{
			var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions = new DocumentConvention
				{
					TransformTypeTagNameToDocumentKeyPrefix = tag => tag,
					FindTypeTagName = type => type.Name
				}
			};
			store.Initialize();

			// Create the Index
			new Product_Search().Execute(store);

			return store;
		}

		private static List<Product> CreateProducts()
		{
			var products = new List<Product>
			{
				new Product
				{
					Id = Guid.NewGuid(),
					Name = "Blu-Ray Player",
					Description = "Latest and greated 3D Blu-Ray Player",
					Created = DateTimeOffset.Now,
					Department = "Electronics",
					Category = "Blu-Ray",
					Attributes = new List<string> {"Single Disc", "Black", "3D"}
				},
				new Product
				{
					Id = Guid.NewGuid(),
					Name = "Stair Fence",
					Description = "Protect your young ones from falling down the stairs.",
					Created = DateTimeOffset.Now,
					Department = "Safety",
					Category = "Children",
					Attributes = new List<string> {"Wood Color", "Top of Stairs", "Removable"}
				},
				new Product
				{
					Id = Guid.NewGuid(),
					Name = "Gigabit Network Switch",
					Description = "Portable 12 Port GB Network Switch.",
					Created = DateTimeOffset.Now,
					Department = "Electronics",
					Category = "Networking",
					Attributes = new List<string> {"12 Port", "Vertical or Horizontal mount"}
				},
				new Product
				{
					Id = Guid.NewGuid(),
					Name = "Cisco 64 Port Switch Commercial Grade",
					Description = "Commercial 64 Port GB Network Switch.",
					Created = DateTimeOffset.Now,
					Department = "Electronics",
					Category = "Networking",
					Attributes = new List<string> {"64 Port", "Rack Mount"}
				},
				new Product
				{
					Id = Guid.NewGuid(),
					Name = "Beolit 12 Airplay",
					Description = "The best portable wireless sounds featuring Apple's Airplay.",
					Created = DateTimeOffset.Now,
					Department = "Electronics",
					Category = "B&O Play",
					Attributes = new List<string> {"Airplay", "USB", "Sweet Sound"}
				}
			};

			return products;
		}

		private class Product_Search : AbstractIndexCreationTask<Product, Product_Search.SearchResult>
		{

			public class SearchResult
			{
				public string Query { get; set; }
				public DateTimeOffset Created { get; set; }
				public string Name { get; set; }
				public string Category { get; set; }
				public string Department { get; set; }
			}

			public Product_Search()
			{
				Map = products =>
				      from product in products
				      select new
				      {
				      	Query = new object[]
				      	{
				      		product.Name,
				      		product.Category,
				      		product.Description,
				      		product.Attributes
				      	},
				      	product.Name,
				      	product.Category,
				      	product.Created,
				      	product.Department
				      };

				Index(x => x.Query, FieldIndexing.Analyzed);
				Index(x => x.Created, FieldIndexing.NotAnalyzed);
				Index(x => x.Name, FieldIndexing.NotAnalyzed);
				Index(x => x.Category, FieldIndexing.NotAnalyzed);
				Index(x => x.Department, FieldIndexing.NotAnalyzed);

			}
		}
	}
}