// -----------------------------------------------------------------------
//  <copyright file="PhilJones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class PhilJones_Search : RavenTestBase
    {
        public PhilJones_Search(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string FirstName { get; set; }
        }

        [Fact]
        public void CanChangeParsingOfSearchQueries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = RavenTestHelper.GetIndexQuery(session.Query<User>()
                        .Search(x => x.FirstName, "*Ore?n*"));

                    Assert.Equal("from 'Users' where search(FirstName, $p0)", query.Query);
                    Assert.Equal(@"*Ore?n*", query.QueryParameters["p0"]);
                }
            }
        }
    }
}
