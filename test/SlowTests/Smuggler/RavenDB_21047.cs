using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class RavenDB_21047 : RavenTestBase
    {
        private const string _docId = "TestObjs/0";
        private const string _changeVector = "A:29931-+oKB0eox0kKFRmpZhHdpYA A:58113-48XOT93s0keG852a0alBCA A:80057-8MXXSiK/j02SPt6WvMBp3g A:34139-J0psmFwcEk+f2D6rB5uSKg " +
                                             "A:53822-RCHmPvRlA0qCqyNWNH7Ufg A:51048-RL5qTmLhV0SwOwKPeOgf4A A:105702-b8TkJER0y0mlX3cdUnPJ7A A:86566-hJl/w96QZ0S1Nrnj73I8kA " +
                                             "A:272221-lF+69deD8UCyPIClNBHGzA A:53182-p3oH6z9j/0WoL33F0GT6dw A:65536-yAs2j2d7EUKMYy7+D+oBEw B:67152-BgN7ypHCN0eZLbWQW4pSbA " +
                                             "B:29926-uAcwGmeZFECexya2bfY6MA B:54928-z0A6qAIgbkeiDwvkcMmsLQ C:65537-3N4Tf0UvQUSG+Zg5q8f41A C:58113-HdKAr6tiA0OhTCazgYuK8A " +
                                             "C:54931-agGw7kyp/EmrroDbznOceQ C:53822-fa7cPVbGKEWWXcPFlgCy0g C:50774-lZTIFvUSQEiC7pboTgiYxg C:51048-qnNDTHlOPUWL/4484fXkzA " +
                                             "C:53182-ts+A2CssQkiaYR3vSyIrlQ D:54929-/15CzUoPak+RM2pwW2arnw";

        public RavenDB_21047(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport)]
        public async Task ShouldThrowWhenRevisionIdExceedsTheMaximumIdLength()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 1 } };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

            var database = await GetDatabase(store.Database);

            Assert.Throws<ArgumentException>(() =>
            {
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        var value = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "TestObjs" }
                        };
                        using var doc = context.ReadObject(value, _docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        database.DocumentsStorage.Put(context, _docId, null, doc, changeVector: _changeVector);
                        tx.Commit();
                    }
                }
            });
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport)]
        public async Task ImportRevisionTombstonesWithBigChangeVectorShouldWork()
        {
            using (var store = GetDocumentStore())
            await using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Smuggler.Data.revisionTombstonesWithBigChangeVector.ravendbdump"))
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var o = session.Load<object>(_docId);
                    Assert.NotNull(o);

                    var revisionsCount = session.Advanced.Revisions.GetCountFor(_docId);
                    Assert.Equal(1, revisionsCount);
                }
            }
        }
    }
}
