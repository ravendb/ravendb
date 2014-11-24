// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1825.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1825 : RavenTest
	{
		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int StoredTotal { get; set; }
		}

		public class Order
		{
			public Order()
			{
				Customers = new List<string>();
			}
			public string Id { get; set; }
			public int OrderItems { get; set; }
			public List<string> Customers { get; set; }
		}

		public class CustomerOrderTotal : AbstractIndexCreationTask<Order, CustomerOrderTotal.ReduceResult>
		{
			public class ReduceResult
			{
				public string CustomerId { get; set; }
				public string CustomerName { get; set; }
				public int TotalOrderItems { get; set; }
			}

			public CustomerOrderTotal()
			{
				Map = orders => from order in orders
								from customer in order.Customers
								select new ReduceResult
								{
									CustomerId = customer,
									CustomerName = LoadDocument<Customer>(customer).Name,
									TotalOrderItems = order.OrderItems
								};

				Reduce = results => results
								.GroupBy(x => x.CustomerId)
								.Select(g => new ReduceResult
								{
									CustomerId = g.Key,
									TotalOrderItems = g.Sum(x => x.TotalOrderItems),
									CustomerName = g.Select(x => x.CustomerName).FirstOrDefault()
								});
			}
		}

		[Fact]
		public void ScriptedIndexResultsPatcherShouldNotPatchDocumentsThatAreIndirectlyReferencedByIndex()
		{
			using (var store = NewDocumentStore(activeBundles: "ScriptedIndexResults"))
			{
				new CustomerOrderTotal().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new ScriptedIndexResults
					{
						Id = ScriptedIndexResults.IdPrefix + (new CustomerOrderTotal().IndexName),
						IndexScript = @"var customer = LoadDocument(this.CustomerId)
if(customer == null) return;
if(customer.StoredTotal != this.TotalOrderItems){
  customer.StoredTotal = this.TotalOrderItems;
  PutDocument(this.CustomerId, customer);
}"
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Store(new Customer { Name = "Customer1", StoredTotal = 100, Id = "customers/1" });
					session.SaveChanges();

					session.Store(new Order { Customers = new List<string> { "customers/1" }, OrderItems = 10 });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var customer = session.Load<Customer>("customers/1");
					Assert.Equal(100, customer.StoredTotal);
				}
			}
		}

		[Fact]
		public void ScriptedIndexResultsPatcherShouldNotPatchDocumentsThatAreIndirectlyReferencedByIndexRaw()
		{
			using (var store = NewDocumentStore(activeBundles: "ScriptedIndexResults"))
			{
				new CustomerOrderTotal().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Customer { Name = "Customer1", StoredTotal = 100, Id = "customers/1" });
					session.SaveChanges();

					session.Store(new Order { Customers = new List<string> { "customers/1" }, OrderItems = 10 });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				var patcher = new ScriptedJsonPatcher(store.SystemDatabase);
				using (var scope = new ScriptedIndexResultsJsonPatcherScope(store.SystemDatabase, new HashSet<string> { "Orders" }))
				{
					var e = Assert.Throws<InvalidOperationException>(() => patcher.Apply(scope, new RavenJObject(), new ScriptedPatchRequest
					{
						Script = @"var customer = LoadDocument('customers/1')
if(customer == null) return;
if(customer.StoredTotal != this.TotalOrderItems){
  customer.StoredTotal = this.TotalOrderItems;
  PutDocument('customers/1', customer);
}"
					}));

					Assert.Equal("Cannot PUT document 'customers/1' to prevent infinite indexing loop. Avoid modifying documents that could be indirectly referenced by index.", e.InnerException.Message);
				}
			}
		}

	}
}