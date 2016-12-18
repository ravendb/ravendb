using Raven.Abstractions.Data;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System.Threading;
    using Raven.Client.Exceptions;

    using Xunit;

    public class RavenDB_578 : ReplicationBase
    {
        public class Person
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string MiddleName { get; set; }
        }

        [Fact]
        public void DeletingConflictedDocumentOnServer1ShouldCauseConflictOnServer2AndResolvingItOnServer2ShouldRecreateDocumentOnServer1()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
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

            TellFirstInstanceToReplicateToSecondInstance();

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Person>("people/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on people/1, conflict must be resolved before the document will be accessible", conflictException.Message);

            TellSecondInstanceToReplicateToFirstInstance();

            store2.DatabaseCommands.Delete("people/1", null);

            conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Load<Person>("people/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on people/1, conflict must be resolved before the document will be accessible", conflictException.Message);

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

                c1.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                store1.DatabaseCommands.Put("people/1", null, c1.DataAsJson, c1.Metadata);
            }

            var p1 = this.WaitForDocument<Person>(store1, "people/1");
            var p2 = this.WaitForDocument<Person>(store2, "people/1");

            Assert.Equal(p1.FirstName, p2.FirstName);
        }
    }
}
