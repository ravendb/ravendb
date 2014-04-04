// -----------------------------------------------------------------------
//  <copyright file="MultiMapWildCardSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.MultiMapSearch
{
	public class MultiMapWildCardSearch : RavenTest
	{
		[Fact]
		public void CanSearch()
		{
			using (var store = NewDocumentStore())
			{
				new AccountSearch().Execute(store);

				using (var session = store.OpenSession())
				{
					int portalId = 1;

					session.Store(new Person
					{
						PortalId = "1",
						FirstName = "firstname",
						LastName = "lastname"
					});


					session.SaveChanges();

					RavenQueryStatistics statistics;
					IQueryable<AccountSearch.ReduceResult> query = session
						.Query<AccountSearch.ReduceResult, AccountSearch>()
						.Statistics(out statistics)
						.Where(x => x.PortalId == portalId)
						.Search(x => x.Query, "*", 1, SearchOptions.And, EscapeQueryOptions.AllowPostfixWildcard)
						.Search(x => x.QueryBoosted, "*", 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard)
						.Customize(x => x.WaitForNonStaleResults());

					var result = query
						.As<Account>()
						.ToList();

					WaitForUserToContinueTheTest(store);

					Assert.Equal(1, result.Count);

				}
			}
		}

	}
}