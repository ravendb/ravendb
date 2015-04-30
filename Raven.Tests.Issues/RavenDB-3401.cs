using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3401 : RavenTestBase
	{
		[Fact]
		public void projections_with_property_rename()
		{
			using (var store = NewDocumentStore())
			{
				var index = new Customers_ByName();
				index.Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
					session.SaveChanges();

					WaitForIndexing(store);

					var customer = session.Query<Customer>(index.IndexName)
						.Select(r => new
						{
							Name = r.Name,
							OtherThanName = r.Address,
							OtherThanName2 = r.Address,
							AnotherOtherThanName = r.Name
						}).Single( );
					{

						Assert.Equal("John", customer.Name);
						Assert.Equal("Tel Aviv", customer.OtherThanName);
						Assert.Equal("Tel Aviv", customer.OtherThanName2);
						Assert.Equal("John", customer.AnotherOtherThanName);

					}
				}
			}
		}

		public class Customer
		{
			public string Name { get; set; }
			public string Address { get; set; }
		}

		public class Customers_ByName : AbstractIndexCreationTask<Customer>
		{
			public Customers_ByName()
			{
				Map = customers => from customer in customers
								   select new
								   {
									   customer.Name
								   };
			}
		}
	}
}


