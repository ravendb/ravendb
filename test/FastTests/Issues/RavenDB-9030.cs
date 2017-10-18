using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9030: RavenTestBase
    {
        [Fact]
        public void InQueryOnMultipleIdsShouldNotThrowTooManyBooleanClauses()
        {
            var numOfIds = 10_000;
            var ids = Enumerable.Range(0, numOfIds).Select(x => x.ToString()).ToArray();
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach (var id in ids)
                    {
                        session.Store(new Document { Id = id });
                    }  
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(numOfIds, session.Query<Document>()
                        .Where(x => x.Id.In(ids))
                        .Select(x => new
                        {
                            x.Id
                        }).Count());
                }
            }
        }

        public class Document
        {
            public string Id { get; set; }
        }
    }
}
