using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using Lextm.SharpSnmpLib.Security;
using Orders;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11770 : RavenTestBase
    {
        [Fact]
        public async Task CanGetRevisionsByBeforeDate()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, 
                    modifyConfiguration: conf => conf.Default.MinimumRevisionsToKeep = 1000);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Fitzchak" }, id);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);

                var fst = db.Time.GetUtcNow();

                db.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);

                for (int i = 0; i < 3; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Fitzchak " + i;
                        await session.SaveChangesAsync();
                    }
                }

                var snd = db.Time.GetUtcNow();

                db.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(15);

                for (int i = 0; i < 3; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Oren " + i;
                        await session.SaveChangesAsync();
                    }
                }


                using (var session = store.OpenAsyncSession())
                {
                    var rev1 = await session.Advanced.Revisions.GetBeforeAsync<Company>(id, fst);
                    Assert.Equal("Fitzchak", rev1.Name);

                    var rev2 = await session.Advanced.Revisions.GetBeforeAsync<Company>(id, snd);
                    Assert.Equal("Fitzchak 2", rev2.Name);

                    var rev3 = await session.Advanced.Revisions.GetBeforeAsync<Company>(id, db.Time.GetUtcNow());
                    Assert.Equal("Oren 2", rev3.Name);
                }
            }
        }
    }
}
