//-----------------------------------------------------------------------
// <copyright file="LinqGitHub147.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class LinqGitHub147 : RavenTest
	{
		public class Order
		{
			public string Id { get; set; }
			public string Customer { get; set; }
			public IList<OrderLine> OrderLines { get; set; }
			public User User { get; set; }


			public Order()
			{
				OrderLines = new List<OrderLine>();
			}
		}

		public class OrderLine
		{
			public string ProductId { get; set; }
			public int Quantity { get; set; }
		}

		public class User
		{
			public string Name { get; set; }
		}

		[Fact]
		public void CanSelectComplexProperty()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new {doc.Customer,doc.User}",
					Stores = { { "User", FieldStorage.Yes } },
				}
												, true);

				using (var s = store.OpenSession())
				{
					var order = new Order
					{
						OrderLines = new List<OrderLine>
						{
							new OrderLine
							{
								ProductId = "productids/3",
								Quantity = 10
							}
						},
						Customer = "two",
						User = new User { Name = "zz2" }
					};
					s.Store(order);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{

					var o3 = from n in s.Query<Order>("test").Customize(x => x.WaitForNonStaleResults())
							 select new { n.User };

					var o4 = o3.First();

					Assert.Equal("zz2", o4.User.Name);

				}
			}
		}
		[Fact]
		public void CanSelectStringProperty()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new {doc.Customer,doc.User.Name}",
					Stores = { { "Customer", FieldStorage.Yes } }
				}
												, true);

				using (var s = store.OpenSession())
				{
					var order = new Order
					{
						OrderLines = new List<OrderLine>
						{
							new OrderLine
							{
								ProductId = "productids/3",
								Quantity = 10
							}
						},
						Customer = "two",
						User = new User { Name = "zz2" }
					};
					s.Store(order);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{

					var o3 = from n in s.Query<Order>("test").Customize(x => x.WaitForNonStaleResults())
							 select new { n.Customer };

					var o4 = o3.First(); 

					Assert.Equal("two", o4.Customer);

				}
			}
		}
	}
}
