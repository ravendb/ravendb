using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NewClientTests.NewClient.Server.Replication;
using Raven.Client.Indexes;
using Raven.Client.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.Server.ServerWide.Context;

namespace Raven.Tests.Issues
{
    public class RavenDB_6259 : ReplicationTestsBase
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
                SetupReplication(master, slave);


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

                var marker = WaitForDocument(slave, "marker");

                using (var session = slave.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).ToList());
                }

                Assert.NotNull(marker);

                DeleteReplication(master,slave);
                

                using (var session = master.OpenAsyncSession())
                {
                    var address = await session.LoadAsync<Address>("addresses/1");
                    address.ZipCode = 2;
                    await session.StoreAsync(address);

                    var markerObject = await session.LoadAsync<dynamic>("marker");
                    await session.StoreAsync(markerObject);
                    await session.SaveChangesAsync();
                }

                using (var session = slave.OpenAsyncSession())
                {
                    session.Delete("addresses/1");
                    await session.StoreAsync(new
                    {
                        Foo = "slaveMarker",
                    }, "slaveMarker").ConfigureAwait(false);
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.ResolveToLatest);

                SetupReplication(master, slave);

                var slaveMarker = WaitForDocument(master, "slaveMarker");
                Assert.NotNull(slaveMarker);

                marker = WaitForDocument(master, "marker");
                Assert.NotNull(marker);

                var slaveServer = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(slave.DefaultDatabase);
                DocumentsOperationContext context;
                using (slaveServer.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var docAndTumbstone = slaveServer.DocumentsStorage.GetDocumentOrTombstone(context, "addresses/1", throwOnConflict: false);
                    Assert.NotNull(docAndTumbstone.Item2);
                    Assert.Null(docAndTumbstone.Item1);
                }


                // here we make sure that we do not enter an infinite loop of indexing (like we did back at 3 era)
                using (var session = slave.OpenSession())
                {
                    long? lastPersonEtag;
                    
                    var person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).First();
                    lastPersonEtag = session.Advanced.GetEtagFor(person);

                    Assert.NotNull(lastPersonEtag);

                    for (var i = 0; i < 5; i++)
                    {
                        person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).First();
                        Thread.Sleep(50);

                        Assert.Equal(lastPersonEtag, session.Advanced.GetEtagFor(person));
                    }
                }

            }
        }
    }
}