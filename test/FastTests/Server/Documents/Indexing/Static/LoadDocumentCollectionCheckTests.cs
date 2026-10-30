using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class LoadDocumentCollectionCheckTests : RavenTestBase
    {
        public LoadDocumentCollectionCheckTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string NonAsciiCollection = "Użytkownicy";

        // longer than the size compared on the stack
        private static readonly string LongCollection = new string('C', 300);

        private class Result
        {
            public string ExactCase { get; set; }
            public string OtherCase { get; set; }
            public string Ascii { get; set; }
            public string LongName { get; set; }
            public string Mismatch { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LoadDocumentComparesTheCollectionCaseInsensitively(Options options)
        {
            using var store = GetDocumentStore(options);

            var commands = store.Commands();
            commands.Put("users/1", null, new { Name = "user" }, new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = NonAsciiCollection });
            commands.Put("companies/1", null, new { Name = "company" }, new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "Companies" });
            commands.Put("longs/1", null, new { Name = "long" }, new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = LongCollection });
            commands.Put("orders/1", null, new { User = "users/1", Company = "companies/1", Long = "longs/1" },
                new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "Orders" });

            const string indexName = "Orders/WithReferences";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps =
                {
                    $@"from o in docs.Orders
select new
{{
    ExactCase = LoadDocument(o.User, ""{NonAsciiCollection}"").Name,
    OtherCase = LoadDocument(o.User, ""{NonAsciiCollection.ToUpperInvariant()}"").Name,
    Ascii = LoadDocument(o.Company, ""companies"").Name,
    LongName = LoadDocument(o.Long, ""{LongCollection.ToLowerInvariant()}"").Name,
    Mismatch = LoadDocument(o.User, ""Companies"").Name
}}"
                },
                Fields = { [Constants.Documents.Indexing.Fields.AllFields] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
            }));

            Indexes.WaitForIndexing(store);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName }));
            Assert.Empty(errors.SelectMany(x => x.Errors));

            using (var session = store.OpenSession())
            {
                var result = session.Advanced
                    .RawQuery<Result>($"from index '{indexName}' select ExactCase, OtherCase, Ascii, LongName, Mismatch")
                    .Single();

                Assert.Equal("user", result.ExactCase);
                Assert.Equal("user", result.OtherCase);
                Assert.Equal("company", result.Ascii);
                Assert.Equal("long", result.LongName);
                Assert.Null(result.Mismatch);
            }
        }
    }
}
