//-----------------------------------------------------------------------
// <copyright file="SimpleReplication.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication
{
    public class SimpleReplication : ReplicationBase
    {
        [Fact]
        public void Can_replicate_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            using (var session = store1.OpenSession())
            {
                session.Store(new Company { Name = "Hibernating Rhinos" });
                session.SaveChanges();
            }

            var company = WaitForDocument<Company>(store2, "companies/1");
            Assert.Equal("Hibernating Rhinos", company.Name);
        }


        [Fact]
        public void Can_replicate_large_number_of_documents_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            using (var session = store1.OpenSession())
            {
                for (int i = 0; i < 150; i++)
                {
                    session.Store(new Company { Name = "Hibernating Rhinos" });
                }
                session.SaveChanges();
            }


            using (var session = store2.OpenSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = RetriesCount * 2;

                bool foundAll = false;
                for (int i = 0; i < RetriesCount; i++)
                {
                    var countFound = 0;
                    for (int j = 0; j < 150; j++)
                    {
                        var company = session.Load<Company>("companies/" + (i + 1));
                        if (company == null)
                            break;
                        countFound++;
                    }
                    foundAll = countFound == 150;
                    if (foundAll)
                        break;
                    Thread.Sleep(100);
                }
                Assert.True(foundAll);
            }
        }

        [Fact]
        public void Will_not_replicate_replicated_documents()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            TellSecondInstanceToReplicateToFirstInstance();
            Company company = null;

            string etag;
            string id;
            using (var session = store1.OpenSession())
            {
                 company = new Company { Name = "Hibernating Rhinos" };
                session.Store(company);
                session.SaveChanges();
                id = company.Id;
                session.Advanced.Clear();
                company = session.Load<Company>(id);
                etag = session.Advanced.GetMetadataFor(company).Value<string>("@etag");
            }



            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store2.OpenSession()) // waiting for document to show up.
                {
                    company = session.Load<Company>(id);
                    if (company != null)
                        break;
                    Thread.Sleep(100);

                }
            }
            Assert.NotNull(company);
            Assert.Equal("Hibernating Rhinos", company.Name);

            // assert that the etag haven't changed (we haven't replicated)
            for (int i = 0; i < 15; i++)
            {
                using (var session = store1.OpenSession())
                {
                    company = session.Load<Company>(id);
                    Assert.Equal(etag, session.Advanced.GetMetadataFor(company).Value<string>("@etag"));
                }
                Thread.Sleep(100);
            }
        }

        [Fact]
        public void Can_replicate_delete_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            using (var session = store1.OpenSession())
            {
                session.Store(new Company { Name = "Hibernating Rhinos" });
                session.SaveChanges();
            }

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
            Assert.Equal("Hibernating Rhinos", company.Name);

            using (var session = store1.OpenSession())
            {
                session.Delete(session.Load<Company>("companies/1"));
                session.SaveChanges();
            }


            Company deletedCompany = null;
            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store2.OpenSession())
                    deletedCompany = session.Load<Company>("companies/1");
                if (deletedCompany == null)
                    break;
                Thread.Sleep(100);
            }
            Assert.Null(deletedCompany);
        }

        [Fact]
        public async Task Will_remove_tombstones_when_deleting_and_creating_new_item_with_same_id()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();
            var item = new Company {Name = "Hibernating Rhinos"};
            using (var session = store1.OpenSession())
            {
                session.Store(item);
                session.SaveChanges();
            }

            WaitForReplication(store2,"companies/1");

            using (var session = store2.OpenSession())
            {
                var company = session.Load<Company>("companies/1");
                Assert.NotNull(company);
                Assert.Equal("Hibernating Rhinos", company.Name);
            }

            using (var session = store1.OpenSession())
            {
                session.Delete(session.Load<Company>("companies/1"));
                session.SaveChanges();
            }

            WaitForDeletionReplication<Company>(store2, "companies/1");
            
            using (var session = store1.OpenSession())
            {
                session.Store(item);
                session.SaveChanges();
            }

            WaitForReplication(store2, "companies/1");

            using (var session = store2.OpenSession())
            {
                var company = session.Load<Company>("companies/1");
                Assert.NotNull(company);
                Assert.Equal("Hibernating Rhinos", company.Name);
            }

            foreach (var ravenDbServer in servers)
            {
                ravenDbServer.SystemDatabase.TransactionalStorage.Batch(
                    accessor => Assert.Null(accessor.Lists.Read("Raven/Replication/Docs/Tombstones", "companies/1")));
            }
        }

        private void WaitForDeletionReplication<T>(DocumentStore store, string Id, int timeout = 5000)
            where T : class 
        {
            T item = null;
            var mre = new ManualResetEventSlim();
            var pollTask = Task.Run(() =>
            {
                do
                {
                    Thread.Sleep(100);
                    using (var session = store.OpenSession())
                        item = session.Load<T>(Id);
                } while (item != null && !mre.IsSet);
            });

            Task.WaitAny(Task.Delay(timeout), pollTask);
            mre.Set();
            Assert.Null(item);
    }
    }
}
