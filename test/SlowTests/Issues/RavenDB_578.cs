using System;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_578 : ReplicationTestsBase
    {
        private class Person
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string MiddleName { get; set; }
        }

        [Fact(Skip = "Waiting for RavenDB-6018")]
        public void DeletingConflictedDocumentOnServer1ShouldCauseConflictOnServer2AndResolvingItOnServer2ShouldRecreateDocumentOnServer1()
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

            var conflicts = WaitUntilHasConflict(store2, "people/1");
            Assert.Equal(2, conflicts.Results.Length);

            SetupReplication(store2, store1);

            conflicts = WaitUntilHasConflict(store1, "people/1");
            Assert.Equal(2, conflicts.Results.Length);

            using (var commands = store2.Commands())
            {
                commands.Delete("people/1", null);
            }

            using (var commands = store1.Commands())
            {
                try
                {
                    commands.Get("people/1");
                }
                catch (DocumentConflictException)
                {
                    throw new NotImplementedException();

                    /*
                    var c1 = commands.Get(e.ConflictedVersionIds[0]);
                    var c2 = commands.Get(e.ConflictedVersionIds[1]);

                    Assert.NotNull(c1);
                    Assert.Null(c2);

                    c1.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                    commands.Put("people/1", null, c1.DataAsJson, c1.Metadata); //FIX ME!
                    */
                }
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
