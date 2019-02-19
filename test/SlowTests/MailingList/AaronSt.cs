// -----------------------------------------------------------------------
//  <copyright file="AaronSt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class AaronSt : RavenTestBase
    {
        [Fact]
        public void CanDoDistinctOnProject()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new MKSession
                        {
                            User = new TesterInfo { AnonymousId = "abc" }
                        });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var z = session.Query<MKSession>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => x.User.AnonymousId)
                        .Distinct()
                        .ToList();

                    Assert.Equal(1, z.Count);
                }
            }
        }

        private class TesterInfo
        {
            public string AnonymousId { get; set; }
            public string ContactEmail { get; set; }
        }

        private class MKSession
        {
            public string Id { get; set; }
            public DateTimeOffset Start { get; set; }
            public DateTimeOffset? End { get; set; }

            public TesterInfo User { get; set; }


            public string ProjectId { get; set; }
            public string VersionId { get; set; }

        }
    }
}
