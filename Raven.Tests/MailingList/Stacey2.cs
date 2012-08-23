using System.Collections.Generic;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Stacey2 : RavenTest
	{
		public class Root
		{
			public string Id { get; set; }
			public Bridge Bridge { get; set; }
		}
		public class Bridge
		{
			public List<string> Aggregates { get; set; }
		}
		public class Aggregate
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void LoadWithInclude()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{

				var aggregate = new Aggregate
				                	{
				                		Name = "First"
				                	};

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					session.Store(aggregate); 
					session.SaveChanges();
				}

				var root = new Root
				           	{
				           		Bridge = new Bridge
				           		         	{
				           		         		Aggregates = new List<string>
				           		         		             	{
				           		         		             		aggregate.Id
				           		         		             	}
				           		         	}
				           	};

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					session.Store(root); session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Load<Aggregate>(1);
					Assert.NotNull(item);
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					var query = session
						.Include("Bridge.Aggregates")
						.Load<Root>(1);

					Assert.NotNull(query);
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					var query = session
						.Include("Bridge.Aggregates")
						.Load<Root>("roots/1");
					var loaded = session.Load<Aggregate>("aggregates/1");

					Assert.NotNull(query);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					var query = session
						.Include("Bridge.Aggregates")
						.Load<Root>(1);
					var loaded = session.Load<Aggregate>("aggregates/1");

					Assert.NotNull(query);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}
	}
}
