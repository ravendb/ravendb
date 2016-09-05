using System.Linq;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Andrew : RavenTestBase
    {
        [Fact]
        public void CanCompile()
        {
            var technologySummaryIndex = new TechnologySummary_Index
            {
                Conventions = new DocumentConvention
                {
                    PrettifyGeneratedLinqExpressions = false
                }
            };

            var indexDefinition = technologySummaryIndex.CreateIndexDefinition();

            Assert.Equal(
                @"docs.Technologies.Where(technology => !technology.__document_id.EndsWith(""/published"")).Select(technology => new {
    TechnologyId = technology.__document_id,
    DrugId = technology.Drug.Id
})",
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
