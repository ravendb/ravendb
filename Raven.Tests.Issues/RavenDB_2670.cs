// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2670.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2670 : RavenTest
	{
		private class Product
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public string Supplier { get; set; }

			public string Category { get; set; }

			public string QuantityPerUnit { get; set; }

			public decimal PricePerUnit{ get; set; }

			public int UnitsInStock { get; set; }

			public int UnitsOnOrder { get; set; }

			public bool Discontinued { get; set; }

			public int ReorderLevel { get; set; }
		}

		private class Products_ByName : AbstractIndexCreationTask<Product>
		{
			public Products_ByName()
			{
				Map = products => from product in products
								  select new
								  {
									  product.Name
								  };

				Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
				Suggestion(x => x.Name);
			}
		}

		[Fact]
		public void MaxSuggestionsShouldWork()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				DeployNorthwind(store, "Northwind");

				new Products_ByName().Execute(store.DatabaseCommands.ForDatabase("Northwind"), store.Conventions);

				WaitForIndexing(store, db: "Northwind");

				using (var session = store.OpenSession("Northwind"))
				{
					var result = session
						.Query<Product, Products_ByName>()
						.Suggest(new SuggestionQuery
						         {
							         Field = "Name", 
									 Term = "<<chaig tof>>", 
									 Accuracy = 0.4f, 
									 MaxSuggestions = 5, 
									 Distance = StringDistanceTypes.JaroWinkler, 
									 Popularity = true
						         });

					Assert.True(result.Suggestions.Length <= 5);
				}
			}
		}
	}
}