using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Data;

namespace SlowTests.Issues
{
    using System.Threading;
    using Raven.Client.Exceptions;

    using Xunit;

    public class RavenDB_578 : ReplicationTestsBase
    {
        public class Person
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string MiddleName { get; set; }
        }

        [Fact(Skip = "Waiting for RavenDB-6018")]
        public async Task DeletingConflictedDocumentOnServer1ShouldCauseConflictOnServer2AndResolvingItOnServer2ShouldRecreateDocumentOnServer1()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();

           
            using (var session = store1.OpenSession())
            {
                session.Store(new Person { FirstName = "John" });
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Person { FirstName = "Doe" });
                session.SaveChanges();
            }
            SetupReplication(store1, store2);

            var conflicts = await WaitUntilHasConflict(store2, "people/1");
            Assert.Equal(2,conflicts["people/1"].Count);

            SetupReplication(store2, store1);
            
            conflicts = await WaitUntilHasConflict(store1, "people/1");
            Assert.Equal(2, conflicts["people/1"].Count);

            store2.DatabaseCommands.Delete("people/1", null);


            try
            {
                store1.DatabaseCommands.Get("people/1");
            }
            catch (ConflictException e)
            {
                var c1 = store1.DatabaseCommands.Get(e.ConflictedVersionIds[0]);
                var c2 = store1.DatabaseCommands.Get(e.ConflictedVersionIds[1]);

                Assert.NotNull(c1);
                Assert.Null(c2);

         //       c1.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                store1.DatabaseCommands.Put("people/1", null, c1.DataAsJson, c1.Metadata);
            }

            var r1 = WaitForDocument(store1, "people/1");
            var r2 = WaitForDocument(store2, "people/1");
            Assert.True(r1);
            Assert.True(r2);

            Person p1, p2;
            using (var session = store1.OpenSession())
            {
                p1 = session.Load<Person>("people/1");
            }
            using (var session = store2.OpenSession())
            {
                p2 = session.Load<Person>("people/1");
            }
            Assert.Equal(p1.FirstName, p2.FirstName);
        }
    }
}
