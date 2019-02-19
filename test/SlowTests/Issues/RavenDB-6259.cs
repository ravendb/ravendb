using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6259 : ReplicationTestBase
    {
        public class PersonAndAddressIndex : AbstractIndexCreationTask<Person, Address>
        {
            public PersonAndAddressIndex()
            {
                Map = people => from person in people
                                let address = LoadDocument<Address>(person.AddressId)
                                let addressId = address != null ? address.Id : null

                                select new
                                {
                                    Id = addressId
                                };
            }
        }

        [Fact]
        public async Task ToLatestConflictResolutionOfTumbstoneAndUpdateShouldNotReviveTubmstone_And_ShouldNotCauseInfiniteIndexingLoop()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                slave.ExecuteIndex(new PersonAndAddressIndex());
                var res = await SetupReplicationAsync(master, slave);


                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(new Address
                    {
                        Id = "addresses/1",
                        Street = "Ha Hashmonaim",
                        ZipCode = 4567
                    }).ConfigureAwait(false);
                    await session.StoreAsync(new Person
                    {
                        Id = "people/1",
                        AddressId = "addresses/1",
                        Name = "Ezekiel"
                    }).ConfigureAwait(false);

                    await session.StoreAsync(new
                    {
                        Foo = "marker"
                    }, "marker").ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                Assert.True(WaitForDocument(slave, "marker"));

                using (var session = slave.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(13))).ToList());
                }
                //delete the replication master->slave
                var removeTaskId = res.First().TaskId;
                await master.Maintenance.SendAsync(new DeleteOngoingTaskOperation(removeTaskId, OngoingTaskType.Replication));

                using (var session = master.OpenAsyncSession())
                {
                    var address = await session.LoadAsync<Address>("addresses/1");
                    address.ZipCode = 2;
                    await session.StoreAsync(address);

                    await session.StoreAsync(new
                    {
                        Foo = "marker"
                    }, "marker2").ConfigureAwait(false);
                    await session.SaveChangesAsync();
                }

                using (var session = slave.OpenAsyncSession())
                {
                    session.Delete("addresses/1");
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.ResolveToLatest);

                await SetupReplicationAsync(master, slave);

                Assert.True(WaitForDocument(slave, "marker2"));

                var slaveServer = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(slave.Database);
                using (slaveServer.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docAndTumbstone = slaveServer.DocumentsStorage.GetDocumentOrTombstone(context, "addresses/1", throwOnConflict: false);
                    Assert.NotNull(docAndTumbstone.Tombstone);
                    Assert.Null(docAndTumbstone.Document);
                }


                // here we make sure that we do not enter an infinite loop of indexing (like we did back at 3 era)
                using (var session = slave.OpenSession())
                {
                    var person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(14))).First();
                    var changeVectorFor = session.Advanced.GetChangeVectorFor(person);

                    Assert.NotNull(changeVectorFor);

                    for (var i = 0; i < 5; i++)
                    {
                        person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(16))).First();
                        Thread.Sleep(50);

                        Assert.Equal(changeVectorFor, session.Advanced.GetChangeVectorFor(person));
                    }
                }

            }
        }
    }
}
