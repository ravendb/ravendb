//-----------------------------------------------------------------------
// <copyright file="ConflictWhenReplicating.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Raven.Tests.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
    public class ConflictWhenReplicating : ReplicationBase
    {
        [Fact]
        public void When_replicating_and_a_document_is_already_there_will_result_in_conflict()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            using(var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Company {Name = "Company2"});
                session.SaveChanges();
            }

            TellFirstInstanceToReplicateToSecondInstance();

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
        }

        [Fact]
        public void Can_resolve_conflict_by_deleting_conflicted_doc()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Company {Name="Second"});
                session.SaveChanges();
            }

            TellFirstInstanceToReplicateToSecondInstance();

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            store2.DatabaseCommands.Delete("companies/1", null);

            foreach (var conflictedVersionId in conflictException.ConflictedVersionIds)
            {
                Assert.Null(store2.DatabaseCommands.Get(conflictedVersionId));
            }
        }

        [Fact]
        public void When_replicating_from_two_different_source_different_documents()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Company { Name = "Company2" });
                session.SaveChanges();
            }

            TellInstanceToReplicateToAnotherInstance(0,2);

            for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
            {
                using (var session = store3.OpenSession())
                {
                    if (session.Load<Company>("companies/1") != null)
                        break;
                    Thread.Sleep(100);
                }
            }

            TellInstanceToReplicateToAnotherInstance(1, 2);

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store3.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
        }

        [Fact]
        public void Can_conflict_on_deletes_as_well()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();
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

            TellInstanceToReplicateToAnotherInstance(0, 2);

            for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
            {
                using (var session = store3.OpenSession())
                {
                    if (session.Load<Company>("companies/1") != null)
                        break;
                    Thread.Sleep(100);
                }
            }

            using (var session = store1.OpenSession())
            {
                session.Delete(session.Load<Company>("companies/1"));
                session.SaveChanges();
            }

            for (int i = 0; i < RetriesCount; i++) // wait for it to NOT show up in the 3rd server
            {
                using (var session = store3.OpenSession())
                {
                    if (session.Load<Company>("companies/1") == null)
                        break;
                    Thread.Sleep(100);
                }
            }


            TellInstanceToReplicateToAnotherInstance(1, 2);

            var conflictException = Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store3.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal("Conflict detected on companies/1, conflict must be resolved before the document will be accessible", conflictException.Message);
        }

        [Fact]
        public void Tombstone_deleted_after_conflict_resolved()
        {
            var store1 = CreateStore(databaseName: Constants.SystemDatabase);
            var store2 = CreateStore(databaseName: Constants.SystemDatabase);
            using (var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            TellFirstInstanceToReplicateToSecondInstance();
            Company company = null;
            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store2.OpenSession())
                {
                    company = session.Load<Company>("companies/1");
                    if (company != null)
                        break;
                    Thread.Sleep(100);
                }
            }
            Assert.NotNull(company);

            //Stop replication
            store1.DatabaseCommands.Delete(Constants.RavenReplicationDestinations, null);
            Assert.Null(store1.DatabaseCommands.Get(Constants.RavenReplicationDestinations));

            using (var session = store1.OpenSession())
            {
                company = session.Load<Company>("companies/1");
                company.Name = "Raven";
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Delete(session.Load<Company>("companies/1"));
                session.SaveChanges();
            }
            servers[1].SystemDatabase.TransactionalStorage.Batch(
            accessor => Assert.NotNull(accessor.Lists.Read("Raven/Replication/Docs/Tombstones", "companies/1")));
    
            TellFirstInstanceToReplicateToSecondInstance();

            Assert.Throws<ConflictException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            using (var session = store2.OpenSession())
            {
                session.Store(new Company(), "companies/1");
                session.SaveChanges();
            }
            servers[1].SystemDatabase.TransactionalStorage.Batch(
                accessor => Assert.Null(accessor.Lists.Read("Raven/Replication/Docs/Tombstones", "companies/1")));
        }
    }
}
