// -----------------------------------------------------------------------
//  <copyright file="ConflictsInEmbeddableStore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
    using Client.Indexes;

    public class ConflictsInEmbeddableStore : ReplicationBase
    {
        [Fact]
        public void ShouldThrowConflictExceptionForLoadingConflictedDocument()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForReplication(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                    }
                });

                Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
                             conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForGettingInfoAboutConflictedDocument()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.DatabaseCommands.Put("companies/1", null, new RavenJObject(), new RavenJObject());
                store2.DatabaseCommands.Put("companies/1", null, new RavenJObject(), new RavenJObject());

                store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForReplication(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    store2.DatabaseCommands.Head("companies/1");
                });

                Assert.Equal(
                    "Conflict detected on companies/1, conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict",
                    conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForQueryingConflictedDocument()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                new RavenDocumentsByEntityName().Execute(store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForReplication(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    }
                });

                Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
                             conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForQueryingConflictedDocument_RemoteStore()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                new RavenDocumentsByEntityName().Execute(store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company {Name = "Company2"});
                    session.SaveChanges();
                }

                store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForReplication(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    }
                });

                Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
                             conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForMultiLoadResultContainsConflictedDocument()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new Company());
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company());
                    session.Store(new Company());
                    session.SaveChanges();
                }

                store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();
                    
                WaitForReplication(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>(new[] { "companies/1", "companies/2" });
                    }
                });

                Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
                             conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForGettingConflictedAttachment()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.DatabaseCommands.PutAttachment("a/1", null, new MemoryStream(), new RavenJObject());
                store2.DatabaseCommands.PutAttachment("a/1", null, new MemoryStream {Capacity = 2}, new RavenJObject());

                store1.DatabaseCommands.PutAttachment("marker", null, new MemoryStream(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForAttachment(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    store2.DatabaseCommands.GetAttachment("a/1");
                });

                Assert.Equal("Conflict detected on a/1, conflict must be resolved before the attachment will be accessible",
                             conflictException.Message);
            }
        }

        [Fact]
        public void ShouldThrowConflictExceptionForGettingInfoAboutConflictedAttachment()
        {
            using (var store1 = CreateEmbeddableStore())
            using (var store2 = CreateEmbeddableStore())
            {
                store1.DatabaseCommands.PutAttachment("a/1", null, new MemoryStream(), new RavenJObject());
                store2.DatabaseCommands.PutAttachment("a/1", null, new MemoryStream {Capacity = 2}, new RavenJObject());

                store1.DatabaseCommands.PutAttachment("marker", null, new MemoryStream(), new RavenJObject());

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForAttachment(store2, "marker");

                var conflictException = Assert.Throws<ConflictException>(() =>
                {
                    store2.DatabaseCommands.HeadAttachment("a/1");
                });

                Assert.Equal(
                    "Conflict detected on a/1, conflict must be resolved before the attachment will be accessible",
                    conflictException.Message);
            }
        }
    }
}
