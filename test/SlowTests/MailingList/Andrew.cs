using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Andrew : RavenTestBase
    {
        public Andrew(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCompile()
        {
            var technologySummaryIndex = new TechnologySummary_Index();

            var indexDefinition = technologySummaryIndex.CreateIndexDefinition();

            Assert.Equal(
                @"docs.Technologies.Where(technology => !Id(technology).EndsWith(""/published"")).Select(technology => new {
    TechnologyId = Id(technology),
    DrugId = technology.Drug.Id
})".Replace("\r\n", Environment.NewLine),
                indexDefinition.Maps.First());
        }

        private class TechnologySummary_Index : AbstractIndexCreationTask<Technology, TechnologySummary>
        {
            public TechnologySummary_Index()
            {
                Map = (technologies => from technology in technologies
                                       where !technology.Id.EndsWith("/published")
                                       select new
                                       {
                                           TechnologyId = technology.Id,
                                           DrugId = technology.Drug.Id,
                                       });

                Reduce = results => from result in results
                                    group result by result.TechnologyId
                                        into g
                                    let rec = g.LastOrDefault()
                                    select
                                        new
                                        {
                                            rec.TechnologyId,
                                            rec.DrugId,
                                        };
            }
        }

        private class TechnologySummary
        {
#pragma warning disable 649
            public string TechnologyId;
            public string DrugId;
#pragma warning restore 649
        }

        private class Technology
        {
#pragma warning disable 649
            public string Id;
            public Drug Drug;
#pragma warning restore 649
        }

        private class Drug
        {
#pragma warning disable 649
            public string Id;
#pragma warning restore 649
        }
    }
}
