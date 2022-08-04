// -----------------------------------------------------------------------
//  <copyright file="RavenDB957.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB957 : RavenTestBase
    {
        public RavenDB957(ITestOutputHelper output) : base(output)
        {
        }

        private class Role
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LazyWithoutSelectNew(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Role { Name = "Admin" });
                    session.SaveChanges();
                }

                // ok
                using (var session = documentStore.OpenSession())
                {
                    var x = session.Query<Role>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .Select(r => new { r.Name })
                        .Lazily()
                        .Value;

                    Assert.Equal("Admin", x.First().Name);
                }

                // fails
                using (var session = documentStore.OpenSession())
                {
                    var x = session.Query<Role>()
                        .Customize(c => c.WaitForNonStaleResults())
                           .Select(r => r.Name)
                           .Lazily()
                           .Value;
                    Assert.Equal("Admin", x.First());
                }
            }
        }
    }
}
