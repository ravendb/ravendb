using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common.Dto;
using Raven.Tests.Core.Replication;
using Raven.Tests.Helpers;
using Xunit;
using User = Raven.Tests.Core.Utils.Entities.User;

namespace Raven.Tests.Issues
{
    public class RavenDB_6259 : RavenReplicationCoreTest
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
        public void DeletedMarkerWithLoadDocumentIndex()
        {
            using (var store = GetDocumentStore())
            {
                new PersonAndAddressIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Address()
                    {
                        Id = "addresses/1",
                        Street = "Ha Hashmonaim",
                        ZipCode = 4567
                    });
                    session.Store(new Person
                    {
                        Id = "people/1",
                        AddressId = "addresses/1",
                        Name = "Ezekiel"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).ToList());
                }


                using (var session = store.OpenSession())
                {
                    var address = session.Load<Address>("addresses/1");

                    var addressMetadata = session.Advanced.GetMetadataFor(address);

                    addressMetadata["Raven-Delete-Marker"] = true;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var address = session.Load<Address>("addresses/1");
                    Assert.Null(address);
                }

                using (var session = store.OpenSession())
                {
                    Etag lastPersonEtag;
                    var person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).First();
                    lastPersonEtag = session.Advanced.GetEtagFor(person);

                    for (var i = 0; i < 5; i++)
                    {
                        person = session.Query<Person, PersonAndAddressIndex>().Customize(x => x.WaitForNonStaleResults()).First();
                        Thread.Sleep(50);
                        Assert.Equal(lastPersonEtag, session.Advanced.GetEtagFor(person));
                    }
                }
            }
        }

        [Fact]
        public async Task ToLocaConflictResolutionOfTumbstoneAndUpdateShouldNotReviveTubmstone()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, destinations: slave);

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "remote"
                    }, "users/1");

                    await session.StoreAsync(new
                    {
                        Foo = "marker"
                    }, "marker");

                    await session.SaveChangesAsync();
                }

                var marker = WaitForDocument(slave, "marker");

                Assert.NotNull(marker);

                using (var session = slave.OpenAsyncSession())
                {
                    await session.StoreAsync(new ReplicationConfig()
                    {
                        DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLatest
                    }, Constants.RavenReplicationConfig);
                    session.Delete("users/1");

                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    user.Count = 2;
                    await session.StoreAsync(user);

                    var markerObject = await session.LoadAsync<RavenJObject>("marker");
                    await session.StoreAsync(markerObject);
                    await session.SaveChangesAsync();
                }

                marker = WaitForDocument(slave, "marker");

                Assert.NotNull(marker);

                var slaveServer = await this.Server.Options.DatabaseLandlord.GetResourceInternal(slave.DefaultDatabase);

                slaveServer.TransactionalStorage.Batch(accessor => {
                    Assert.Null(accessor.Documents.DocumentMetadataByKey("users/1"));
                });

            }
        }

    }
}
