using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class RavenArrayQueryInputTests : RavenTestBase
    {
        public class Document
        {
            public string Id { get; set; }
        }

        public class TransformResult
        {
            public string Id { get; set; }
            public int[] Array { get; set; }
        }

        public class TestTransformer : AbstractTransformerCreationTask<Document>
        {
            public TestTransformer()
            {
                TransformResults = documents => from d in documents
                    let index = Array.IndexOf(Parameter("array").Value<string>().Split(','), d.Id)
                    orderby index
                    select new
                    {
                        Id = d.Id,
                        Array = new[] {1, 2, 3}
                    };
            }
        }
        [Fact]
        public void CanUseArrayAsQueryInput()
        {
            using (var store = NewDocumentStore())
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
                    var ids = new[] {"documents/3", "documents/1", "documents/2"};

                    session.Load<TestTransformer, TransformResult>(ids, c => c.AddTransformerParameter("array", string.Join(",", ids)));
                }
            }
        }
    }
}
