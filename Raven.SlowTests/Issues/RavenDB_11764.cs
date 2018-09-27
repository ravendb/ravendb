using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_11764 : RavenTestBase
    {
        [Fact]
        public void GetDocumentsWithIdStartingWith_WhenFilteringLeadThePageDocumentsCollectingToEndInTheMiddleOfUnfilteredPage_NextStartShouldBeOneAfterTheLastDocument()
        {
            const int pageSize = 5;
            const int documentAmount = pageSize + 2;

            using (var documentStore = NewDocumentStore())
            {
                for (var i = 0; i < documentAmount; i++)
                {
                    documentStore.SystemDatabase.Documents.Put("FooBar" + i, null, new RavenJObject(), new RavenJObject(), null);
                }
                
                var nextStart = 0;
                var documents = documentStore.SystemDatabase.Documents;
                documents.GetDocumentsWithIdStartingWith("FooBar", string.Empty, "1*", 0, pageSize, CancellationToken.None, ref nextStart);

                Assert.Equal(6, nextStart);
            }
        }

        [Fact]
        public void LoadStartingWithAndIsLastPage_WhenFilteringLeadThePageDocumentsCollectingToEndInTheMiddleOfUnfilteredPageAndRepeatUntilLastPage_ShouldLoadAllMatches()
        {
            const int pageSize = 5;
            const int expected = pageSize + 1;

            using (var store = NewDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { Id = "Employee/" + "A", Name = "Bob" });
                for (var i = 0; i < expected; i++)
                {
                    session.Store(new Employee { Id = "Employee/" + "B" + i, Name = "Bob" });
                }
                session.SaveChanges();

                var start = 0;
                var allDocuments = new List<Employee>();
                var pagingInformation = new RavenPagingInformation();

                do
                {
                    IEnumerable<Employee> documents = session.Advanced.LoadStartingWith<Employee>(
                        keyPrefix: "Employee/",
                        matches: "B*",
                        start: start,
                        pageSize: pageSize,
                        pagingInformation: pagingInformation
                    );

                    allDocuments.AddRange(documents);
                    start += pageSize;
                }
                while (pagingInformation.IsLastPage() == false);

                Assert.Equal(expected, allDocuments.Count); 
            }
        }

        private class Employee
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}