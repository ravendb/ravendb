using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Client.Document.DTC;
using Raven.Client.Exceptions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6626 : RavenTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task LoadOperation_should_not_throw()
        {
            using (var server = GetNewServer(requestedStorage:"esent"))
            using (var store = NewRemoteDocumentStore(ravenDbServer: server,requestedStorage:"esent"))
            {
                store.TransactionRecoveryStorage = new VolatileOnlyTransactionRecoveryStorage();
                var database = await server.Server.GetDatabaseInternal(store.DefaultDatabase);

                database.PrepareTransaction("1");
                using (var session = store.OpenSession())
                {
                    var entity = new User {Name = "John Doe"};
                    session.Store(entity, "users/1");
                   
                    session.SaveChanges();
                    database.InFlightTransactionalState.AddDocumentInTransaction("users/1",
                        Etag.Empty,
                        RavenJObject.FromObject(entity),
                        new RavenJObject(),
                        new Raven.Abstractions.Data.TransactionInformation { Id = "1" },
                        Etag.Empty,
                        new DummyUuidGenerator());
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.AllowNonAuthoritativeInformation = false;

                    //note: the issue was that LoadAsync in this use-case was throwing StackOverflow
                    var ae = Assert.Throws<AggregateException>(() => session.LoadAsync<User>("users/1").Wait());

                    Assert.NotNull(ae.InnerException);
                    Assert.IsType<NonAuthoritativeInformationException>(ae.InnerException);
                }
            }

        }
    }
}
