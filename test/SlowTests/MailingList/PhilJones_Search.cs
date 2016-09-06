// -----------------------------------------------------------------------
//  <copyright file="PhilJones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.MailingList
{
    public class PhilJones_Search : RavenTestBase
    {
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
                    Assert.Equal(@"FirstName:(\*Ore\?n\*)", session.Query<User>()
                        .Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.EscapeAll)
                        .ToString());

                    Assert.Equal(@"FirstName:(*Ore?n*)", session.Query<User>()
                        .Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.RawQuery)
                        .ToString());


                    Assert.Equal(@"FirstName:(*Ore\?n*)", session.Query<User>()
                        .Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.AllowAllWildcards)
                        .ToString());

                    Assert.Equal(@"FirstName:(\*Ore\?n*)", session.Query<User>()
                        .Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.AllowPostfixWildcard)
                        .ToString());
                }
            }
        }
    }
}