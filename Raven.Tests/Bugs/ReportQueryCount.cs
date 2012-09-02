//-----------------------------------------------------------------------
// <copyright file="ReportQueryCount.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class ReportQueryCount : RavenTest
	{
		[Fact]
		public void CanFindOutWhatTheQueryTotalCountIs()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					RavenQueryStatistics stats;
					s.Query<User>()
						.Statistics(out stats)
						.Where(x => x.Name == "ayende")
						.ToArray();

					Assert.Equal(0, stats.TotalResults);
					Assert.False(stats.IsStale);
				}
			}
		}

		[Fact]
		public void CanGetQueryTimestamp()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User{Name = "ayende"});
					s.SaveChanges();

					RavenQueryStatistics stats;
					s.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Statistics(out stats)
						.Where(x => x.Name == "ayende")
						.ToArray();

					Assert.Equal(1, stats.TotalResults);
					Assert.False(stats.IsStale);
					Assert.NotEqual(DateTime.MinValue, stats.Timestamp);
				}
			}
		}
	}
}
