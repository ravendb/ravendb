//-----------------------------------------------------------------------
// <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Server;
using Raven.Tests.Bugs.TransformResults;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Includes : RemoteClientTest, IDisposable
	{
		private readonly IDocumentStore store;
		private readonly RavenDbServer server;

		public Includes()
		{
			server = GetNewServer(8079, GetPath(DbName));

			store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize();

			store.DatabaseCommands.PutIndex("Orders/ByName",
											new IndexDefinition
											{
												Map = "from doc in docs.Orders select new { doc.Name }"
											});

			using (var session = store.OpenSession())
			{

				for (int i = 0; i < 15; i++)
				{
					var customer = new Customer
					{
						Email = "ayende@ayende.com",
						Name = "Oren"
					};

					session.Store(customer);

					session.Store(new Order
					{
						Customer = new DenormalizedReference
						{
							Id = customer.Id,
							Name = customer.Name
						},
						Name = (i + 1).ToString()
					});
				}
				session.SaveChanges();
			}
		}

		[Fact]
		public void CanIncludeWithSingleLoad()
		{
			using (var session = store.OpenSession())
			{
				var order = session
					.Include("Customer.Id")
					.Load<Order>("orders/1");

				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer = session.Load<Customer>(order.Customer.Id);

				Assert.NotNull(customer);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void FromLuceneQuery()
		{
			using (var session = store.OpenSession())
			{
				var orders = session.Advanced.LuceneQuery<Order>()
					.Include(x => x.Customer.Id)
					.WaitForNonStaleResults()
					.WhereEquals("Name", "3")
					.ToArray();

				Assert.Equal(1, orders.Length);
				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer = session.Load<Customer>(orders[0].Customer.Id);
				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}


		[Fact]
		public void FromLinqQuery()
		{
			using (var session = store.OpenSession())
			{
				var orders = session.Query<Order>()
					.Customize(x =>
					{
						x.Include<Order>(z => z.Customer.Id);
						x.WaitForNonStaleResults();
					})
					.Where(x => x.Name == "3")
					.ToArray();

				Assert.Equal(1, orders.Length);
				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer = session.Load<Customer>(orders[0].Customer.Id);
				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void IncludeMissingProperty()
		{
			new Answers_ByAnswerEntity().Execute(store);
			var answerId = ComplexValuesFromTransformResults.CreateEntities(store);
			using (var session = store.OpenSession())
			{
				var views = session
					.Query<Answer, Answers_ByAnswerEntity>()
					.Customize(x => x.Include("Fppbar").WaitForNonStaleResults())
					.Where(x => x.Id == answerId)
					.ToArray();
			}
		}

		[Fact]
		public void IncludeOnMapReduce()
		{
			new Votes_ByAnswerEntity().Execute(store);
			var answerId = ComplexValuesFromTransformResults.CreateEntities(store);
			using (var session = store.OpenSession())
			{
				var vote = session
					.Query<AnswerVote, Votes_ByAnswerEntity>()
					.Customize(x => x.Include("QuestionId").WaitForNonStaleResults())
					.Where(x => x.AnswerId == answerId)
					.FirstOrDefault();

				session.Load<Question>(vote.QuestionId);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void CanIncludeWithSingleLoad_UsingExpression()
		{
			using (var session = store.OpenSession())
			{
				var order = session
					.Include<Order>(x => x.Customer.Id)
					.Load("orders/1");

				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer = session.Load<Customer>(order.Customer.Id);

				Assert.NotNull(customer);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void CanIncludeWithQuery()
		{
			using (var session = store.OpenSession())
			{
				var orders = session
					.Advanced.LuceneQuery<Order>("Orders/ByName")
					.WaitForNonStaleResults()
					.Include("Customer.Id")
					.Take(2)
					.ToList();

				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer1 = session.Load<Customer>(orders[0].Customer.Id);
				var customer2 = session.Load<Customer>(orders[1].Customer.Id);

				Assert.NotNull(customer1);
				Assert.NotNull(customer2);

				Assert.NotSame(customer1, customer2);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void CanIncludeExtensionWithQuery()
		{
			using (var session = store.OpenSession())
			{
				var orders = session.Advanced
					.LuceneQuery<Order>("Orders/ByName")
					.WaitForNonStaleResults()
					.Include(o => o.Customer.Id)
					.Take(2)
					.ToList();

				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer1 = session.Load<Customer>(orders[0].Customer.Id);
				var customer2 = session.Load<Customer>(orders[1].Customer.Id);

				Assert.NotNull(customer1);
				Assert.NotNull(customer2);

				Assert.NotSame(customer1, customer2);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void CanIncludeWithMultiLoad()
		{
			using (var session = store.OpenSession())
			{
				var orders = session
					.Include("Customer.Id")
					.Load<Order>("orders/1", "orders/2");

				Assert.Equal(1, session.Advanced.NumberOfRequests);

				var customer1 = session.Load<Customer>(orders[0].Customer.Id);
				var customer2 = session.Load<Customer>(orders[1].Customer.Id);

				Assert.NotNull(customer1);
				Assert.NotNull(customer2);

				Assert.NotSame(customer1, customer2);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		public override void Dispose()
		{
			store.Dispose();
			server.Dispose();
			ClearDatabaseDirectory();
			base.Dispose();
		}

		public class Order
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public DenormalizedReference Customer { get; set; }
		}

		public class DenormalizedReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }

		}
	}
}
