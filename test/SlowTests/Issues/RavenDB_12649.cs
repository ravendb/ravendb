using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12649 : RavenTestBase
    {
        private const string StatusPostfix = "/status";

        [Fact]
        public void ProjectionWithLoadFails()
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = documentId,
                        Status = Status.Passed
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var result = GetResults(s.Query<Document>()).SingleOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(Status.Passed, result.Status);
                    Assert.False(result.StatusOverridden);

                    s.Store(new StatusDocument
                    {
                        Id = documentId + StatusPostfix,
                        DocumentId = documentId,
                        OverriddenStatus = Status.Failed
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var result = GetResults(s.Query<Document>().Customize(x => x.WaitForNonStaleResults())).SingleOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal(Status.Failed, result.Status);
                    Assert.True(result.StatusOverridden);
                }
            }
        }

        private static IQueryable<Result> GetResults(IQueryable<Document> documents)
        {
            return from document in documents
                   let status = RavenQuery.Load<StatusDocument>(document.Id + StatusPostfix)

                   let overriddenStatus = status != null && status.OverriddenStatus != Status.NotSet

                   select new Result
                   {
                       Id = document.Id,
                       Status = overriddenStatus ? status.OverriddenStatus : document.Status,
                       StatusOverridden = overriddenStatus && status.OverriddenStatus != document.Status,
                   };
        }

        private enum Status
        {
            NotSet = 0,
            Passed = 1,
            Failed = 2
        }

        private class Result
        {
            public string Id { get; set; }
            public Status Status { get; set; }
            public bool StatusOverridden { get; set; }
        }

        private class Document
        {
            public string Id { get; set; }
            public Status Status { get; set; }
        }

        private class StatusDocument
        {
            public string Id { get; set; }
            public string DocumentId { get; set; }
            public Status OverriddenStatus { get; set; }
        }
    }
}

