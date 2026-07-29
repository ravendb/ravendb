using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_26938 : ClusterTestBase
{
    public RavenDB_26938(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void SideBySideReplacementFromResetShouldSurviveDatabaseRecordChange()
    {
        using (var store = GetDocumentStore())
        {
            new DocIndex().Execute(store); // simple map index

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", StrVal = "value" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            // stop indexing so the side-by-side replacement cannot complete and swap
            store.Maintenance.Send(new StopIndexingOperation());

            store.Maintenance.Send(new ResetIndexOperation(nameof(DocIndex), IndexResetMode.SideBySide));

            var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + nameof(DocIndex);

            var names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
            Assert.Contains(replacementName, names); // replacement exists

            // any database record change; here: deploying an unrelated index
            new OtherIndex().Execute(store);

            names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
            Assert.Contains(replacementName, names); // the replacement should still be there
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }

    private class Other
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from doc in docs
                select new { doc.StrVal };
        }
    }

    private class OtherIndex : AbstractIndexCreationTask<Other>
    {
        public OtherIndex()
        {
            Map = others => from other in others
                select new { other.StrVal };
        }
    }
}
