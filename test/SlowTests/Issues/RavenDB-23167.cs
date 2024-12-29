using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Table = Voron.Data.Tables.Table;

namespace SlowTests.Issues
{
    public class RavenDB_23167 : RavenTestBase
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
            
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 3, empty: false, expectedEtag: 2);
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 0, empty: true);
            AssertSeekBackwardForFixedSizeTrees(db, endEtag: 10, empty: false, expectedEtag: 6);

            AssertSeekBackward(db, "foo/bar", endEtag: 3, empty: false, expectedEtag: 2);
            AssertSeekBackward(db, "foo/bar", endEtag: 0, empty: true);
            AssertSeekBackward(db, "foo/bar", endEtag: 10, empty: false, expectedEtag: 6);
            AssertSeekBackward(db, "foo/bar", endEtag: long.MaxValue, empty: false, expectedEtag: 6);

            AssertSeekBackward(db, "foo/bar", endEtag: 5, empty: false, expectedEtag: 2);
            AssertSeekBackward(db, "foo/bar1", endEtag: 5, empty: false, expectedEtag: 4);
            AssertSeekBackward(db, "foo/ba", endEtag: 5, empty: true);

            AssertSeekBackwardAfterAllKeys(db, "foo/bar", empty: false, expectedEtag: 6);
            AssertSeekBackwardAfterAllKeys(db, "foo/ba", empty: true);
        }

        private static void AssertSeekBackwardForFixedSizeTrees(DocumentDatabase db, long endEtag, bool empty, long? expectedEtag = null)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var table = new Table(RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                var voronIndex = RevisionsStorage.RevisionsSchema.FixedSizeIndexes[RevisionsStorage.AllRevisionsEtagsSlice];
                var tvhs = table.SeekBackwardFrom(voronIndex, endEtag);
                var revisions = tvhs.Select(tvh => RevisionsStorage.TableValueToRevision(context, ref tvh.Reader, DocumentFields.ChangeVector)).ToList();

                if (empty)
                {
                    Assert.Empty(revisions);
                    if (expectedEtag.HasValue)
                        throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
                }
                else
                {
                    Assert.NotEmpty(revisions);
                    var lastLocalEtag = revisions[0].Etag;
                    Assert.True(endEtag >= lastLocalEtag, $"endEtag {endEtag}, lastLocalEtag: {lastLocalEtag}");

                    if(expectedEtag.HasValue)
                        Assert.Equal(expectedEtag.Value, lastLocalEtag);
                }
            }
        }

        private static void AssertSeekBackward(DocumentDatabase db, string id, long endEtag, bool empty, long? expectedEtag = null)
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
                {
                    Assert.Empty(revisions);
                    if (expectedEtag.HasValue)
                        throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
                }
                else
                {
                    Assert.NotEmpty(revisions);
                    var lastLocalEtag = revisions[0].Etag;
                    Assert.True(endEtag >= lastLocalEtag, $"endEtag {endEtag}, lastLocalEtag: {lastLocalEtag}");

                    if (expectedEtag.HasValue)
                        Assert.Equal(expectedEtag.Value, lastLocalEtag);
                }
            }
        }

        private static void AssertSeekBackwardAfterAllKeys(DocumentDatabase db, string id, bool empty = false, long? expectedEtag = null)
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
                {
                    Assert.Empty(revisions);
                    if (expectedEtag.HasValue)
                        throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
                }
                else
                {
                    Assert.NotEmpty(revisions);

                    if (expectedEtag.HasValue)
                    {
                        var lastLocalEtag = revisions[0].Etag;
                        Assert.Equal(expectedEtag.Value, lastLocalEtag);
                    }
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
