// -----------------------------------------------------------------------
//  <copyright file="CanExcludeNullItems.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class CanExcludedNullItems : RavenTestBase
	{
		[Fact]
		public void WillSupportLast()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new Student {Email = "support@hibernatingrhinos.com"});
					session.Store(new Student {Email = "support@hibernatingrhinos.com", PersonId = 1});
					session.SaveChanges();
				}

				new Students_ByEmailDomain().Execute(store);
				WaitForUserToContinueTheTest(store);
				using (IDocumentSession session = store.OpenSession())
				{
					var results =
						session.Query<Students_ByEmailDomain.Result,Students_ByEmailDomain>()
						       .Customize(customization => customization.WaitForNonStaleResults())
						       .ToList();

					Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
					Assert.Equal(1, results.Count);
				}
			}
		}

		private class Student
		{
			public string Email { get; set; }
			public long? PersonId { get; set; }
		}

		private class Students_ByEmailDomain : AbstractIndexCreationTask<Student,
			                                       Students_ByEmailDomain.Result>
		{
			public Students_ByEmailDomain()
			{
				Map = students => from student in students
				                  where student.PersonId != null
				                  select new
				                  {
					                  EmailDomain = student.Email.Split('@').Last(),
					                  Count = 1,
				                  };

				Reduce = results => from result in results
				                    group result by result.EmailDomain
				                    into g
				                    select new
				                    {
					                    EmailDomain = g.Key,
					                    Count = g.Sum(r => r.Count),
				                    };
			}

			public class Result
			{
				public string EmailDomain { get; set; }
				public int Count { get; set; }
			}
		}
	}
}