using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5927 : RavenNewTestBase
    {
        private class RavenConflictDocumentsTransformer : AbstractTransformerCreationTask
        {
            public override string TransformerName
            {
                get
                {
                    return "Raven/ConflictDocumentsTransformer";
                }
            }

            public override TransformerDefinition CreateTransformerDefinition(bool prettify = true)
            {
                return new TransformerDefinition
                {
                    Name = TransformerName,
                    TransformResults = @"
from result in results
select new {
    Id = result[""__document_id""],
    ConflictDetectedAt = result[""@metadata""].Value<DateTime>(""@last-modified""),
                EntityName = result[""@metadata""][""@collection""],
                Versions = result.Conflicts.Select(versionId =>
                {
                    var version = LoadDocument(versionId);
                    return new
                    {
                        Id = versionId,
                        SourceId = version[""@metadata""][""Raven-Replication-Source""]
                    };
                })
            }
"
                };
            }
        }

        [Fact]
        public void ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                new RavenConflictDocumentsTransformer().Execute(store);
            }
        }
    }
}