using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_718 : RavenTest
	{
		public class TestAccount
		{
			public string Id { get; set; }
			public List<string> Sites { get; set; }
			public List<TestSubscription> Subscriptions { get; set; }

			public TestAccount()
			{
				Sites = new List<string>();
				Subscriptions = new List<TestSubscription>();
			}
		}

		public class TestSubscription
		{
			public DateTime EndDate { get; set; }
		}

		public class TestSite
		{
			public string Id { get; set; }
			public string Hostname { get; set; }
		}

		public class SitesWithSubscriptions : AbstractMultiMapIndexCreationTask<SitesWithSubscriptions.Result>
		{
			public SitesWithSubscriptions()
			{
				AddMap<TestSite>(sites => from site in sites
										  select new
										  {
											  SiteId = site.Id,
											  Hostname = site.Hostname,
											  SubscriptionEndDate = DateTime.MinValue
										  });

				AddMap<TestAccount>(accounts => from account in accounts
												from siteId in account.Sites
												from subscription in account.Subscriptions
												select new
												{
													SiteId = siteId,
													Hostname = (string)null,
													SubscriptionEndDate = subscription.EndDate
												});

				Reduce = results => from result in results
									group result by result.SiteId into g
									select new
									{
										SiteId = g.Key,
										Hostname = g.Select(x => x.Hostname).Where(x => x != null).FirstOrDefault(),
										SubscriptionEndDate = g.Max(x => x.SubscriptionEndDate)
									};
			}

			public class Result
			{
				public string SiteId { get; set; }
				public string Hostname { get; set; }
				public DateTime SubscriptionEndDate { get; set; }
			}
		}

		public RavenDB_718()
		{
			Store = NewDocumentStore();
			new SitesWithSubscriptions().Execute(Store);
			using (var session = Store.OpenSession())
			{
				var site = new TestSite { Hostname = "www.dev.com" };
				session.Store(site);

				var account = new TestAccount();
				account.Subscriptions.Add(new TestSubscription { EndDate = DateTime.UtcNow });
				account.Sites.Add(site.Id);
				session.Store(account);

				session.SaveChanges();
			}
		}

		protected EmbeddableDocumentStore Store { get; set; }

		public override void Dispose()
		{
			Store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void DateTimeShouldWorkInUTCUsingReduce()
		{
			using (var session = Store.OpenSession())
			{
				var results = session.Query<SitesWithSubscriptions.Result, SitesWithSubscriptions>()
					.Customize(x => x.WaitForNonStaleResults())
					.ToList();

				Assert.Empty(Store.DocumentDatabase.Statistics.Errors);

				Assert.Equal(1, results.Count());
				Assert.Equal(DateTime.UtcNow.Date, results.First().SubscriptionEndDate.Date);
				Assert.Equal(DateTimeKind.Utc, results.First().SubscriptionEndDate.Kind);
			}
		}
	}
}