using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using SlowTests.Voron.Bugs;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;
using static Lucene.Net.Search.SpanFilterResult;

namespace SlowTests.Issues
{
    public class RavenDB_23167 : ReplicationTestBase
    {
        public RavenDB_23167(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task SeekBackwardGivingGreaterKeysThanTheLastKeyThatPassed()
        {
            using var store = GetDocumentStore();
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                }
            };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Create a doc with 2 revisions
            using (var session = store.OpenAsyncSession())
            {
                var person = new Person
                {
                    Name = "Name1"
                };
                await session.StoreAsync(person, "foo/bar");
                await session.SaveChangesAsync();
                // cv: A:1, (local) etag: 2
            }

            using (var session = store.OpenAsyncSession())
            {
                var person = new Person
                {
                    Name = "Name2"
                };
                await session.StoreAsync(person, "foo/bar1");
                await session.SaveChangesAsync();
                // cv: A:3, etag: 4
            }


            using (var session = store.OpenAsyncSession())
            {
                var person = new Person
                {
                    Name = "Name3"
                };
                await session.StoreAsync(person, "foo/bar");
                await session.SaveChangesAsync();
                // cv: A:5, etag: 6
            }
            
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 3, empty: false);
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 0, empty: true);
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 10, empty: false);
            AssertSeekBackward(db, "foo/bar", endEtag: 3, empty: false);
            AssertSeekBackward(db, "foo/bar", endEtag: 0, empty: true);
            AssertSeekBackward(db, "foo/bar", endEtag: 10, empty: false);
            AssertSeekBackward(db, "foo/bar", endEtag: long.MaxValue, empty: false);
            AssertSeekBackwardAfterAllKeys(db, "foo/bar", empty: false);
        }

        private static void AssertSeekBackwardForFixedSizeTrees(DocumentDatabase db, long endEtag, bool empty = false)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = new Table(RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                var voronIndex = RevisionsStorage.RevisionsSchema.FixedSizeIndexes[RevisionsStorage.AllRevisionsEtagsSlice];
                var tvhs = table.SeekBackwardFrom(voronIndex, endEtag);
                var revisions = tvhs.Select(tvh => RevisionsStorage.TableValueToRevision(context, ref tvh.Reader, DocumentFields.ChangeVector)).ToList();

                if (empty)
                    Assert.Empty(revisions);
                else
                {
                    Assert.NotEmpty(revisions);
                    var lastLocalEtag = revisions[0].Etag;
                    Assert.True(endEtag >= lastLocalEtag, $"endEtag {endEtag}, lastLocalEtag: {lastLocalEtag}");
                }
            }
        }

        private static void AssertSeekBackward(DocumentDatabase db, string id, long endEtag, bool empty = false)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, id, out var idSlice))
            using (db.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, idSlice, out Slice prefixSlice))
            using (RevisionsStorage.GetKeyWithEtag(context, idSlice, endEtag, out var compoundPrefix))
            {
                var table = new Table(RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                var voronIndex = RevisionsStorage.RevisionsSchema.Indexes[RevisionsStorage.IdAndEtagSlice];
                var trvs = table.SeekBackwardFrom(voronIndex, prefixSlice, compoundPrefix, 0);
                var revisions = trvs.Select(tvr => RevisionsStorage.TableValueToRevision(context, ref tvr.Result.Reader, DocumentFields.ChangeVector)).ToList();
                
                if (empty)
                    Assert.Empty(revisions);
                else
                {
                    Assert.NotEmpty(revisions);
                    var lastLocalEtag = revisions[0].Etag;
                    Assert.True(endEtag >= lastLocalEtag, $"endEtag {endEtag}, lastLocalEtag: {lastLocalEtag}");
                }
            }
        }

        private static void AssertSeekBackwardAfterAllKeys(DocumentDatabase db, string id, bool empty = false)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, id, out var idSlice))
            using (db.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, idSlice, out Slice prefixSlice))
            {
                var table = new Table(RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                var voronIndex = RevisionsStorage.RevisionsSchema.Indexes[RevisionsStorage.IdAndEtagSlice];
                var trvs = table.SeekBackwardFrom(voronIndex, prefixSlice, Slices.AfterAllKeys, 0);
                var revisions = trvs.Select(tvr => RevisionsStorage.TableValueToRevision(context, ref tvr.Result.Reader, DocumentFields.ChangeVector)).ToList();

                if (empty)
                    Assert.Empty(revisions);
                else
                {
                    Assert.NotEmpty(revisions);
                    var lastLocalEtag = revisions[0].Etag;
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string AddressId { get; set; }
        }
    }
}
