using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12068 : RavenTestBase
    {
        [Fact]
        public void TernaryOperatorPrecedence()
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = documentId,
                        PlotTypeDecision =  false,
                        NullableDecision = true,
                        ResultLookup = new Dictionary<PlotType, DocumentItem>
                        {
                            { PlotType.Type1, new DocumentItem { Mean = 123 } },
                            { PlotType.Type2, new DocumentItem { Mean = 123 } },
                            { PlotType.Type3, new DocumentItem { Mean = 123 } },
                            { PlotType.Type4, new DocumentItem { Mean = 123 } },
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var queryable = s.Query<Document>()
                        .Where(x => x.Id == documentId)
                        .Select(x => new Result
                        {
                            ProjectedMean = x.ResultLookup != null
                                ? x.ResultLookup[x.NullableDecision ?? true
                                    ? (x.PlotTypeDecision ? PlotType.Type2 : PlotType.Type4)
                                    : (x.PlotTypeDecision ? PlotType.Type1 : PlotType.Type3)].Mean
                                : null,
                        });
                    
                    var stats = queryable
                        .SingleOrDefault();

                    Assert.NotNull(stats);
                    Assert.Equal(123, stats.ProjectedMean);
                }
            }
        }

        private class Result
        {
            public decimal? ProjectedMean { get; set; }
        }


        private enum PlotType
        {
            Type1 = 1,
            Type2 = 2,
            Type3 = 3,
            Type4 = 4
        }

        private class Document
        {
            public string Id { get; set; }
            public bool? NullableDecision { get; set; }
            public bool PlotTypeDecision { get; set; }
            public Dictionary<PlotType, DocumentItem> ResultLookup { get; set; }
        }

        private class DocumentItem
        {
            public decimal? Mean { get; set; }
        }
    }
}
