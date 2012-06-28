using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenDbPutTest : RavenTest
	{
		public class Book
		{
			public string Id { get; set; }
			public Guid OldId { get; set; }
			public string Name { get; set; }

		}

		[Fact]
		public void strangely_puts_after_just_a_query()
		{
			using(GetNewServer())
			using (var documentStore = new DocumentStore { Url = "http://localhost:8079/" })
			{
				documentStore.Conventions.DefaultQueryingConsistency = Raven.Client.Document.ConsistencyOptions.QueryYourWrites;

				documentStore.Initialize();

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Book { Name = "Hello",    });
					session.Store(new Book { Name = "Baby",   });
					session.Store(new Book { Name = "Deer",  });

					session.SaveChanges();
				}

				// Now try querying the index and see the strange PUT requests.
				using (var session = documentStore.OpenSession())
				{

					var query = session.Query<Book>()
						.Customize(x=>x.WaitForNonStaleResults());


					var results = query.ToList();

					var old = session.Advanced.NumberOfRequests;
					session.SaveChanges();
					Assert.Equal(old, session.Advanced.NumberOfRequests);
				}
			}
		}
	}
}