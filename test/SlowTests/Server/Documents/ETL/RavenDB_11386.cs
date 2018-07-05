using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_11386 : EtlTestBase
    {
        [Theory]
        [InlineData("Users")]
        [InlineData(null)]
        public async Task Should_remove_counter_tombstone_after_deleting_it_on_destination(string collection)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                if (collection == null)
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    AddEtl(src, dest, collection, script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > (collection == null ? 1 : 0)); // if applyToAllDocuments = true then we also send hilo doc

                using (var session = src.OpenSession())
                {
                    var user = new User();

                    session.Store(user);

                    session.CountersFor(user).Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    Assert.NotNull(user);

                    var counter = session.CountersFor(user).Get("likes");

                    Assert.NotNull(counter);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var counter = session.CountersFor("users/1-A").Get("likes");

                    Assert.Null(counter);
                }

                // counter was deleted on destination side
                // we can remove it from the storage

                var db = await GetDatabase(src.Database);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, 128).ToList();

                    Assert.Equal(1, tombstones.Count);
                    Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[0].Type);
                }

                await db.TombstoneCleaner.ExecuteCleanup();

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, 128).ToList();
                    Assert.Equal(0, tombstones.Count);
                }
            }
        }
    }
}
