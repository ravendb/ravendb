using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Config;
using Raven.Server.Utils.Enumerators;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13972_32_bits : RavenTestBase
    {
        [Fact]
        public void CanStreamDocumentsWithPulsatingReadTransaction()
        {
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager)] = "true"

                }
            }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                }
            }))
            {
                int numberOfUsers = 2 * PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10;

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        bulk.Store(new User(), "users/" + i);
                    }
                }

                AssertAllDocsStreamed(store, numberOfUsers);

                AssertAllDocsStreamedWithPaging(store, numberOfUsers);

                AssertAllStartsWithDocsStreamed(store, numberOfUsers);

                AssertAllStartsWithDocsStreamedWithPaging(store, numberOfUsers);

                // deleting some docs

                var deleted = 0;

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        if (i % 2 != 0)
                            continue;

                        session.Delete("users/" + i);

                        deleted++;
                    }

                    session.SaveChanges();
                }

                AssertAllDocsStreamed(store, numberOfUsers - deleted);

                AssertAllDocsStreamedWithPaging(store, numberOfUsers - deleted);

                AssertAllStartsWithDocsStreamed(store, numberOfUsers - deleted);

                AssertAllStartsWithDocsStreamedWithPaging(store, numberOfUsers - deleted);

                //using (var session = store.OpenSession())
                //{
                //    for (int i = PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded - 2; i < PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 2; i++)
                //    {
                //        session.Delete("users/" + i);

                //        deleted++;
                //    }

                //    session.SaveChanges();
                //}
            }
        }

        private static void AssertAllStartsWithDocsStreamedWithPaging(DocumentStore store, int numberOfUsers)
        {
            using (var session = store.OpenSession())
            {
                var start = 10;

                var en = session.Advanced.Stream<User>("users/", start: start);

                var count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers - start, count);

                var take = numberOfUsers / 2 + 3;

                en = session.Advanced.Stream<User>("users/", start: start, pageSize: take);

                count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(take, count);
            }
        }

        private static void AssertAllStartsWithDocsStreamed(DocumentStore store, int numberOfUsers)
        {
            using (var session = store.OpenSession())
            {
                var en = session.Advanced.Stream<User>("users/");

                var count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers, count);
            }
        }

        private static void AssertAllDocsStreamedWithPaging(DocumentStore store, int numberOfUsers)
        {
            using (var session = store.OpenSession())
            {
                var start = 11;

                var en = session.Advanced.Stream<User>((string)null, start: start);

                var count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers - start, count);

                var take = numberOfUsers / 2 + 3;

                en = session.Advanced.Stream<User>((string)null, start: start, pageSize: take);

                count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(take, count);
            }
        }

        private static void AssertAllDocsStreamed(DocumentStore store, int numberOfUsers)
        {
            using (var session = store.OpenSession())
            {
                var en = session.Advanced.Stream<User>((string)null);

                var count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers, count);
            }
        }
    }
}
