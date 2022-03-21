using System;
using System.Linq;
using FastTests;
using Orders;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11794 : RavenTestBase
    {
        public RavenDB_11794(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseLastModifiedInAutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "CF"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .RawQuery<Company>($"from Companies where @metadata.@last-modified >= '{now1.GetDefaultRavenFormat(now1.Kind == DateTimeKind.Utc)}'")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                }

                var now2 = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .RawQuery<Company>($"from Companies where @metadata.@last-modified >= '{now1.GetDefaultRavenFormat(now1.Kind == DateTimeKind.Utc)}'")
                        .ToList();

                    Assert.Equal(2, companies.Count);

                    companies = session
                        .Advanced
                        .RawQuery<Company>($"from Companies where @metadata.@last-modified >= '{now2.GetDefaultRavenFormat(now2.Kind == DateTimeKind.Utc)}'")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("HR", companies[0].Name);

                    companies = session
                        .Advanced
                        .RawQuery<Company>($"from Companies where @metadata.@last-modified >= '{now1.GetDefaultRavenFormat(now1.Kind == DateTimeKind.Utc)}' order by @metadata.@last-modified")
                        .ToList();

                    Assert.Equal(2, companies.Count);
                    Assert.Equal("CF", companies[0].Name);
                    Assert.Equal("HR", companies[1].Name);

                    companies = session
                        .Advanced
                        .RawQuery<Company>($"from Companies where @metadata.@last-modified >= '{now1.GetDefaultRavenFormat(now1.Kind == DateTimeKind.Utc)}' order by @metadata.@last-modified desc")
                        .ToList();

                    Assert.Equal(2, companies.Count);
                    Assert.Equal("HR", companies[0].Name);
                    Assert.Equal("CF", companies[1].Name);
                }
            }
        }
    }
}
