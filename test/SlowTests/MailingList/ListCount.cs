using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class ListCount : RavenTestBase
    {
        public ListCount(ITestOutputHelper output) : base(output)
        {
        }

        private class Location
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public List<Guid> Properties { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Location
                    {
                        Properties = new List<Guid>
                        {
                            Guid.NewGuid(),
                            Guid.NewGuid()
                        },
                        Name = "Ayende"
                    });
                    session.SaveChanges();

                    var result = session.Query<Location>()
                        .Where(x => x.Name.StartsWith("ay"))
                        .Select(x => new
                        {
                            x.Name,
                            x.Properties.Count
                        }).ToList();

                    Assert.Equal("Ayende", result[0].Name);
                    Assert.Equal(2, result[0].Count);
                }
            }
        }
    }
}
