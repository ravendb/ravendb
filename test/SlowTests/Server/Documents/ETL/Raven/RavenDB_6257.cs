using System;
using System.Linq;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_6257 : EtlTestBase
    {
        public RavenDB_6257(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Collection_specific_etl_process_is_aware_of_processed_tombstones()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User());

                    session.Store(new Order());

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1-A");
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var db = GetDatabase(src.Database).Result;

                var etlProcess = db.EtlLoader;

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    var tombstoneEtags = etlProcess.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                    Assert.Equal(tombstones.First(x => x.Collection.CompareTo("Users") == 0).Etag, tombstoneEtags["Users"]);
                }
            }
        }

        [Fact]
        public void All_docs_etl_process_is_aware_of_processed_tombstones()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new string[0], script: null, applyToAllDocuments: true);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= 4); // 2 docs and 2 HiLos

                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.Store(new Order());

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= 6);

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1-A");
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var db = GetDatabase(src.Database).Result;

                var etlProcess = db.EtlLoader;

                DocumentsOperationContext context;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    var tombstoneEtags = etlProcess.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                    Assert.Equal(tombstones.Max(x => x.Etag), tombstoneEtags[Constants.Documents.Collections.AllDocumentsCollection]);
                }
            }
        }
    }
}
