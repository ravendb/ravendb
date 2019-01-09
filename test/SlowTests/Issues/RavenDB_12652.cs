using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12652 : RavenTestBase
    {
        private const string StatusPostfix = "/status";

        [Fact]
        public void IndexComparingEnumPropertiesShouldNotError()
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore())
            {
                new StatusIndex().Execute(store);

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
                    var result = s.Query<StatusIndex.Result, StatusIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .As<Document>()
                        .SingleOrDefault(x => x.Id == documentId);

                    Assert.NotNull(result);

                    s.Store(new StatusDocument
                    {
                        Id = documentId + StatusPostfix,
                        DocumentId = documentId,
                        OverriddenStatus = Status.Failed
                    });

                    s.SaveChanges();

                    result = s.Query<StatusIndex.Result, StatusIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.StatusOverridden && x.Status == Status.Failed)
                        .As<Document>()
                        .SingleOrDefault(x => x.Id == documentId);

                    Assert.NotNull(result);
                }
            }
        }

        private enum Status
        {
            NotSet = 0,
            Passed = 1,
            Failed = 2
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

        private class StatusIndex : AbstractIndexCreationTask<Document>
        {
            internal class Result
            {
                public string Id { get; set; }
                public Status Status { get; set; }
                public bool StatusOverridden { get; set; }
            }

            public StatusIndex()
            {
                Map = documents => from document in documents
                                   let status = LoadDocument<StatusDocument>(document.Id + StatusPostfix)
                                   let statusSet = status != null && status.OverriddenStatus != Status.NotSet
                                   select new
                                   {
                                       document.Id,
                                       Status = statusSet ? status.OverriddenStatus : document.Status,
                                       StatusOverridden = statusSet && status.OverriddenStatus != document.Status,
                                   };

            }
        }
    }
}

