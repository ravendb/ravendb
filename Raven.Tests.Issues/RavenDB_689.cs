using Raven.Abstractions.Data;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System;
    using System.IO;
    using System.Threading;

    using Raven.Abstractions.Extensions;
    using Raven.Client;
    using Raven.Client.Exceptions;
    using Raven.Json.Linq;

    using Xunit;
    using Xunit.Sdk;

    public class RavenDB_689 : ReplicationBase
    {
        public class User
        {
            public long Tick { get; set; }
        }

        /// <summary>
        /// 3 machines
        ///   1 -> 3
        ///   1 -> 2
        ///   2 -> 1
        ///- Create users/1 on 1, let it replicate to 2, 3
        ///- Disconnect 1 from the network
        ///- Update users/1 on 2, let ir replicate 3
        ///- Update user/1 on 1 (still disconnected)
        ///- Disconnect 2, reconnect 1
        ///- Let the conflict happen on 3
        ///- Reconnect 2, let the conflict flow to 2 or 1
        ///- Resolve the conflict on the conflicted machine
        ///- Reconnect 3, should replicate the conflict resolution
        ///	**** Right now, it conflict on the conflict, which shouldn't be happening.
        ///	**** Show detect that this is successful resolution
        /// </summary>
        [Fact]
        public void TwoMastersOneSlaveAttachmentReplicationIssue()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2, store3);
            SetupReplication(store2.DatabaseCommands, store1, store3);

            store1.DatabaseCommands.PutAttachment("users/1", null, new MemoryStream(new byte[] { 1 }), new RavenJObject());

            WaitForAttachment(store2, "users/1");
            WaitForAttachment(store3, "users/1");

            RemoveReplication(store1.DatabaseCommands);
            RemoveReplication(store2.DatabaseCommands);

            SetupReplication(store2.DatabaseCommands, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            var attachment = store2.DatabaseCommands.GetAttachment("users/1");
            store2.DatabaseCommands.PutAttachment("users/1", attachment.Etag, new MemoryStream(new byte[] { 2 }), attachment.Metadata);

            WaitFor(store3.DatabaseCommands.GetAttachment, "users/1", a => Assert.Equal(new byte[] { 2 }, a.Data().ReadData()));

            WaitFor(store1.DatabaseCommands.GetAttachment, "users/1", a => Assert.Equal(new byte[] { 1 }, a.Data().ReadData()));
            
            attachment = store1.DatabaseCommands.GetAttachment("users/1");
            store1.DatabaseCommands.PutAttachment("users/1", attachment.Etag, new MemoryStream(new byte[] { 3 }), attachment.Metadata);

            RemoveReplication(store2.DatabaseCommands);
            SetupReplication(store1.DatabaseCommands, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    store3.DatabaseCommands.GetAttachment("users/1");
                    Thread.Sleep(100);
                }
            });

            Assert.Equal("Conflict detected on users/1, conflict must be resolved before the attachment will be accessible", conflictException.Message);

            RemoveReplication(store1.DatabaseCommands);
            RemoveReplication(store2.DatabaseCommands);
            SetupReplication(store1.DatabaseCommands, store2, store3);
            SetupReplication(store2.DatabaseCommands, store1, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            IDocumentStore store;

            try
            {
                conflictException = Assert.Throws<ConflictException>(() =>
                {
                    for (int i = 0; i < RetriesCount; i++)
                    {
                        store1.DatabaseCommands.GetAttachment("users/1");
                        Thread.Sleep(100);
                    }
                });

                store = store1;
            }
            catch (ThrowsException)
            {
                conflictException = Assert.Throws<ConflictException>(() =>
                {
                    for (int i = 0; i < RetriesCount; i++)
                    {
                        store2.DatabaseCommands.GetAttachment("users/1");
                        Thread.Sleep(100);
                    }
                });

                store = store2;
            }

            Assert.Equal("Conflict detected on users/1, conflict must be resolved before the attachment will be accessible", conflictException.Message);

            byte[] expectedData = null;

            try
            {
                store.DatabaseCommands.GetAttachment("users/1");
            }
            catch (ConflictException e)
            {
                var c1 = store.DatabaseCommands.GetAttachment(e.ConflictedVersionIds[0]);
                var c2 = store.DatabaseCommands.GetAttachment(e.ConflictedVersionIds[1]);

                expectedData = c1.Data().ReadData();

                store.DatabaseCommands.PutAttachment("users/1", null, new MemoryStream(expectedData), c1.Metadata);
            }

            WaitFor(store1.DatabaseCommands.GetAttachment, "users/1", a => Assert.Equal(expectedData, a.Data().ReadData()));
            WaitFor(store2.DatabaseCommands.GetAttachment, "users/1", a => Assert.Equal(expectedData, a.Data().ReadData()));
            WaitFor(store3.DatabaseCommands.GetAttachment, "users/1", a => Assert.Equal(expectedData, a.Data().ReadData()));
        }

        [Fact]
        public void TwoMastersOneSlaveDocumentReplicationIssue()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2, store3);
            SetupReplication(store2.DatabaseCommands, store1, store3);

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Tick = 1 });
                session.SaveChanges();
            }

            WaitForDocument(store2.DatabaseCommands, "users/1");
            WaitForDocument(store3.DatabaseCommands, "users/1");

            RemoveReplication(store1.DatabaseCommands);
            RemoveReplication(store2.DatabaseCommands);

            SetupReplication(store2.DatabaseCommands, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            using (var session = store2.OpenSession())
            {
                var user = session.Load<User>("users/1");
                user.Tick = 2;
                session.Store(user);
                session.SaveChanges();
            }

            WaitFor(
                id =>
                {
                    using (var session = store1.OpenSession())
                    {
                        return session.Load<User>(id);
                    }
                },
                "users/1",
                doc => Assert.Equal(1, doc.Tick));

            WaitFor(
                id =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Load<User>(id);
                    }
                },
                "users/1",
                doc => Assert.Equal(2, doc.Tick));

            using (var session = store1.OpenSession())
            {
                var user = session.Load<User>("users/1");
                user.Tick = 3;
                session.Store(user);
                session.SaveChanges();
            }

            RemoveReplication(store2.DatabaseCommands);
            SetupReplication(store1.DatabaseCommands, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store3.OpenSession())
                    {
                        session.Load<User>("users/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on users/1, conflict must be resolved before the document will be accessible", conflictException.Message);

            RemoveReplication(store1.DatabaseCommands);
            RemoveReplication(store2.DatabaseCommands);
            SetupReplication(store1.DatabaseCommands, store2, store3);
            SetupReplication(store2.DatabaseCommands, store1, store3);

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            Thread.Sleep(1000); // give the replication task more time to get new destinations setup

            IDocumentStore store;

            try
            {
                conflictException = Assert.Throws<ConflictException>(
                    () =>
                    {
                        for (int i = 0; i < RetriesCount; i++)
                        {
                            using (var session = store1.OpenSession())
                            {
                                session.Load<User>("users/1");
                                Thread.Sleep(100);
                            }
                        }
                    });

                store = store1;
            }
            catch (ThrowsException)
            {
                conflictException = Assert.Throws<ConflictException>(
                    () =>
                    {
                        for (int i = 0; i < RetriesCount; i++)
                        {
                            using (var session = store2.OpenSession())
                            {
                                session.Load<User>("users/1");
                                Thread.Sleep(100);
                            }
                        }
                    });

                store = store2;
            }

            Assert.Equal("Conflict detected on users/1, conflict must be resolved before the document will be accessible", conflictException.Message);

            long expectedTick = -1;

            try
            {
                store.DatabaseCommands.Get("users/1");
            }
            catch (ConflictException e)
            {
                var c1 = store.DatabaseCommands.Get(e.ConflictedVersionIds[0]);
                var c2 = store.DatabaseCommands.Get(e.ConflictedVersionIds[1]);

                c1.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                store.DatabaseCommands.Put("users/1", null, c1.DataAsJson, c1.Metadata);

                expectedTick = long.Parse(c1.DataAsJson["Tick"].ToString());
            }

            WaitFor(
                id =>
                {
                    using (var session = store1.OpenSession())
                    {
                        return session.Load<User>(id);
                    }
                },
                "users/1",
                doc => Assert.Equal(expectedTick, doc.Tick));

            WaitFor(
                id =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return session.Load<User>(id);
                    }
                },
                "users/1",
                doc => Assert.Equal(expectedTick, doc.Tick));

            WaitFor(
                id =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Load<User>(id);
                    }
                },
                "users/1",
                doc => Assert.Equal(expectedTick, doc.Tick));
        }

        private T WaitFor<T>(Func<string, T> getFunction, string entityId, Action<T> assert)
        {
            Exception lastException = null;

            for (var i = 0; i < RetriesCount; i++)
            {
                try
                {
                    var entity = getFunction(entityId);
                    assert(entity);

                    return entity;
                }
                catch (Exception e)
                {
                    lastException = e;
                }

                Thread.Sleep(100);
            }

            if (lastException != null)
            {
                throw lastException;
            }

            throw new Exception("Assert failed from unknown reason.");
        }
    }
}
