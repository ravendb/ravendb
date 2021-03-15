using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16350 : RavenTestBase
    {
        public RavenDB_16350(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Indexes_Should_Ignore_NewLine_Characters_WhenComparing_AdditionalSources()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName_Linux().Execute(store);
                new Companies_Count_Linux().Execute(store);

                WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                new Companies_ByName_Windows().Execute(store);
                new Companies_Count_Windows().Execute(store);

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 25));
                Assert.Equal(2, indexes.Length);
            }
        }

        private class Companies_ByName_Windows : AbstractIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        "from company in docs.Companies \r\n select new { company.Name }"
                    },
                    AdditionalSources =
                    {
                        { "Source", "\r\n" }
                    }
                };
            }
        }

        private class Companies_ByName_Linux : AbstractIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        "from company in docs.Companies \n select new { company.Name }"
                    },
                    AdditionalSources =
                    {
                        { "Source", "\n" }
                    }
                };
            }
        }

        private class Companies_Count_Windows : AbstractIndexCreationTask
        {
            public override string IndexName => "Companies/Count";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        "from company in docs.Companies select new { company.Name, Count = 1 }"
                    },
                    Reduce = "from r in results \r\n group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x => x.Count) }",
                    AdditionalSources =
                    {
                        { "Source", "\r\n" }
                    }
                };
            }
        }

        private class Companies_Count_Linux : AbstractIndexCreationTask
        {
            public override string IndexName => "Companies/Count";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        "from company in docs.Companies select new { company.Name, Count = 1 }"
                    },
                    Reduce = "from r in results \n group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x => x.Count) }",
                    AdditionalSources =
                    {
                        { "Source", "\n" }
                    }
                };
            }
        }
    }
}
