using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16378_LastModifiedDate : ReplicationTestBase
    {
        public RavenDB_16378_LastModifiedDate(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldPreserveLastModifiedDateOfReplicatedTombstone()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_dst"
            }))
            {
                var srcDb = await GetDatabase(src.Database);
                var dstDb = await GetDatabase(dst.Database);

                srcDb.Time.UtcDateTime = () => DateTime.UtcNow.Add(TimeSpan.FromDays(-30));

                using (var session = src.OpenSession())
                {
                    session.Store(new { Foo = "delete-marker" }, "delete-marker");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Delete("delete-marker");

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
