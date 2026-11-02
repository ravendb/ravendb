using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_27473 : RavenTestBase
    {
        public RavenDB_27473(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DocumentWithoutAnyIndexedFieldMustNotMarkTheNextDocumentsFieldsAsNonExisting(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var commands = store.Commands())
            {
                var metadata = new Dictionary<string, object> { [Raven.Client.Constants.Documents.Metadata.Collection] = "Items" };

                // items/2 has none of the indexed fields, so the converter skips it. Its "missing" fields used to leak
                // into items/3, which then got the 'non existing' marker for N on top of its real value and was sorted
                // among the documents that have no N.
                commands.Put("items/1", null, new { S = "a", N = 1 }, metadata);
                commands.Put("items/2", null, new { }, metadata);
                commands.Put("items/3", null, new { S = "a", N = 3 }, metadata);
                commands.Put("items/4", null, new { S = "a", N = 4 }, metadata);
                commands.Put("items/5", null, new { S = "a" }, metadata);
                commands.Put("items/6", null, new { S = "a", N = 6 }, metadata);
            }

            new Items_BySAndN().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var rows = session.Advanced
                    .RawQuery<Row>("from index 'Items/BySAndN' order by N select id() as Id, N")
                    .ToList();

                Assert.Equal(new[] { "items/1", "items/3", "items/4", "items/5", "items/6" }, rows.Select(x => x.Id).OrderBy(x => x));

                Assert.Equal(new[] { "items/1", "items/3", "items/4", "items/6" }, rows.Where(x => x.N != null).Select(x => x.Id));

                // the only document without N goes first or last - never among the documents that have a value
                var missingAt = rows.FindIndex(x => x.N == null);
                Assert.True(missingAt == 0 || missingAt == rows.Count - 1,
                    $"items/5 (no N) landed at position {missingAt} of {rows.Count}: {string.Join(", ", rows.Select(x => $"{x.Id}({x.N})"))}");
            }
        }

        private class Row
        {
            public string Id { get; set; }

            public long? N { get; set; }
        }

        private class Items_BySAndN : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Items/BySAndN",
                    Maps = { "from d in docs.Items select new { d.S, d.N }" }
                };
            }
        }
    }
}
