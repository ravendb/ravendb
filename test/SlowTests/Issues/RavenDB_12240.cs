using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12240 : RavenTestBase
    {
        [Fact]
        public void CanUseMapReduceWithDocumentsContainingCultureSpecificCharactersInId()
        {
            var collectionName = "GesprächTemplates";     // not working
            //var collectionName = "GespraechTemplates";  // working

            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new GesprächTemplateIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new GesprächTemplate
                    {
                        Id = $"{collectionName}/1",
                        GruppeContentId = "GruppeContents/1",
                        Name = "Template 1"
                    });

                    session.Store(new GesprächTemplate
                    {
                        Id = $"{collectionName}/2",
                        GruppeContentId = "GruppeContents/2",
                        Name = "Template 1"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<GesprächTemplateIndex.Result, GesprächTemplateIndex>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        private class GesprächTemplateIndex : AbstractIndexCreationTask<GesprächTemplate, GesprächTemplateIndex.Result>
        {
            public class Result
            {
                public string GruppeContentId { get; set; }
                public int Count { get; set; }
            }

            public GesprächTemplateIndex()
            {
                Map = gesprächTemplates => from gesprächTemplate in gesprächTemplates
                                           select new
                                           {
                                               GruppeContentId = gesprächTemplate.GruppeContentId,
                                               Count = 1
                                           };

                Reduce = results => from result in results
                                    group result by new { result.GruppeContentId } into g
                                    select new
                                    {
                                        GruppeContentId = g.Key.GruppeContentId,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class GesprächTemplate
        {
            public string Id { get; set; }
            public string GruppeContentId { get; set; }
            public string Name { get; set; }
        }
    }
}
