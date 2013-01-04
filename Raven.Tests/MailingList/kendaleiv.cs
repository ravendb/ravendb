// -----------------------------------------------------------------------
//  <copyright file="kendaleiv .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class kendaleiv : RavenTest
	{
		public class Company
		{
			public string Id { get; set; }
			public string CompanySalesId { get; set; }
			public IEnumerable<Contact> Contacts { get; set; }
		}

		public class Contact
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
		}

		public class CompanySales
		{
			public string Id { get; set; }
			public decimal SalesTotal { get; set; }
		}

		public class CompanyContactIndex : AbstractIndexCreationTask<Company, CompanyContactIndex.IndexResult>
		{
			public CompanyContactIndex()
			{
				Map = companies => from company in companies
								   from contact in company.Contacts
								   select new
								   {
									   CompanyId = company.Id,
									   company.CompanySalesId,
									   contact.FirstName,
									   contact.LastName,
								   };

				Store("CompanyId", FieldStorage.Yes);
				Store("CompanySalesId", FieldStorage.Yes);
				Store("FirstName", FieldStorage.Yes);
				Store("LastName", FieldStorage.Yes);

				TransformResults = (database, results) => from result in results
														  let companySales = database.Load<CompanySales>(result.CompanySalesId)
														  select new
														  {
															  result.CompanyId,
															  result.FirstName,
															  result.LastName,
															  companySales.SalesTotal,
														  };
			}

			public class IndexResult
			{
				public string CompanyId { get; set; }
				public string FirstName { get; set; }
				public string LastName { get; set; }
				public decimal SalesTotal { get; set; }
				public string CompanySalesId { get; set; }
			}
		}

		[Fact]
		public void ReturnsCorrectTake()
		{
			const int NUMBER_OF_COMPANIES = 100;

			using (var store = NewDocumentStore())
			{
				new CompanyContactIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new CompanySales()
					{
						Id = "companiesSales/test",
						SalesTotal = 100m
					});

					for (var i = 0; i < NUMBER_OF_COMPANIES; i++)
					{
						session.Store(new Company()
						{
							CompanySalesId = "companiesSales/test",
							Contacts = new List<Contact>()
							{
								new Contact() { FirstName = "John", LastName = "Doe" },
								new Contact() { FirstName = "Jane", LastName = "Doe" }
							}
						});
					}

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var items = session.Query<Company, CompanyContactIndex>()
						.Customize(x => x.WaitForNonStaleResults())
						.As<CompanyContactIndex.IndexResult>()
						.Take(10)
						.ToList();

					Assert.Equal(10, items.Count); // items.Count is 20
				}
			}
		}
	}
}
