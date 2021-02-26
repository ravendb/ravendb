using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16293 : RavenTestBase
    {
        public RavenDB_16293(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUpdateIndexWithAdditionalSources()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                new Companies_ByName_Without().Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_Without>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }

                new Companies_ByName_With().Execute(store);

                WaitForIndexing(store, allowErrors: true);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_With>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }
            }
        }

        [Fact]
        public void CanUpdateIndexWithAdditionalSources_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                new Companies_ByName_Without_JavaScript().Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_Without_JavaScript>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }

                new Companies_ByName_With_JavaScript().Execute(store);

                WaitForIndexing(store, allowErrors: true);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_With_JavaScript>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }
            }
        }

        private class Companies_ByName_Without : AbstractIndexCreationTask<Company>
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from company in docs.Companies select new { Name = Helper.GetName(company.Name) }" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            public static class Helper
            {
                public static string GetName(string name)
                {
                    return ""HR"";
                }
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_With : AbstractIndexCreationTask<Company>
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from company in docs.Companies select new { Name = Helper.GetName(company.Name) }" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            public static class Helper
            {
                public static string GetName(string name)
                {
                    return name;
                }
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_Without_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"map('Companies', function (c){ return { Name: getName(c.Name); };})" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            function getName(name)
            {
                return 'HR';
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_With_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"map('Companies', function (c){ return { Name: getName(c.Name); };})" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            function getName(name)
            {
                return name;
            }
"
                        }
                    }
                };
            }
        }
    }
}
