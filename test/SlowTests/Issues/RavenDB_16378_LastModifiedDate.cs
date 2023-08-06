using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16378_LastModifiedDate : ReplicationTestBase
    {
        public RavenDB_16378_LastModifiedDate(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldPreserveLastModifiedDateOfReplicatedTombstone(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dst = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_dst"
            }))
            {
                var srcDb = await GetDocumentDatabaseInstanceForAsync(src, options.DatabaseMode, "marker");
                var dstDb = await GetDocumentDatabaseInstanceForAsync(dst, options.DatabaseMode, "marker");

                srcDb.Time.UtcDateTime = () => DateTime.UtcNow.Add(TimeSpan.FromDays(-30));

                using (var session = src.OpenSession())
                {
                    session.Store(new { Foo = "delete-marker" }, "delete-marker$marker");
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Delete("delete-marker$marker");
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new { Foo = "marker" }, "marker");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(src, dst);

                WaitForDocument(dst, "marker");

                using (dstDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctxDst))
                using (ctxDst.OpenReadTransaction())
                {
                    var tombstonesInDst = dstDb.DocumentsStorage.GetTombstonesFrom(ctxDst, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(1, tombstonesInDst.Count);

                    Assert.NotEqual(tombstonesInDst[0].LastModified.Date, dstDb.Time.GetUtcNow().Date);

                    using (srcDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctxSrc))
                    using (ctxSrc.OpenReadTransaction())
                    {
                        var tombstonesInSrc = dstDb.DocumentsStorage.GetTombstonesFrom(ctxDst, 0, 0, int.MaxValue).ToList();
                        Assert.Equal(1, tombstonesInSrc.Count);

                        Assert.Equal(tombstonesInSrc[0].LastModified, tombstonesInDst[0].LastModified);
                    }
                }
            }
        }
    }
}
