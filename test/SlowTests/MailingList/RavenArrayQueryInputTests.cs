using System;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenArrayQueryInputTests : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
        }

        private class TransformResult
        {
            public string Id { get; set; }
            public int[] Array { get; set; }
        }

        private class TestTransformer : AbstractTransformerCreationTask<Document>
        {
            public TestTransformer()
            {
                TransformResults = documents => from d in documents
                                                let index = Array.IndexOf(Parameter("array").Value<string>().Split(','), d.Id)
                                                orderby index
                                                select d;
            }
        }

        [Fact]
        public void CanUseArrayAsQueryInput()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new TestTransformer();
                transformer.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Document());
                    session.Store(new Document());
                    session.Store(new Document());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ids = new[] { "documents/3", "documents/1", "documents/2" };

                    session.Load<TestTransformer, TransformResult>(ids, c => c.AddTransformerParameter("array", string.Join(",", ids)));
                }
            }
        }
    }
}
