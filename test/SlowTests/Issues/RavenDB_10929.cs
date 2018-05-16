using FastTests;
using Orders;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10929 : RavenTestBase
    {
        [Fact]
        public void CanUpdateDatabaseRecord()
        {
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                var etag = record.Etag;
                Assert.NotNull(record);
                Assert.True(etag > 0);
                Assert.False(record.Disabled);

                record.Disabled = true;

                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, etag));

                record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                Assert.NotNull(record);
                Assert.True(record.Etag > etag);
                Assert.True(record.Disabled);

                Assert.Throws<ConcurrencyException>(() => store.Maintenance.Server.Send(new CreateDatabaseOperation(record)));

                Assert.Throws<DatabaseDisabledException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Company { Name = "MS" });
                        session.SaveChanges();
                    }
                });
            }
        }
    }
}
