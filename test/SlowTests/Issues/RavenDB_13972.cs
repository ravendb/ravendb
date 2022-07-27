using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Utils.Enumerators;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public abstract class RavenDB_13972 : RavenTestBase
    {
        protected RavenDB_13972(ITestOutputHelper output) : base(output)
        {
        }
        
        protected static async Task CanExportWithPulsatingReadTransaction_ActualTest(int numberOfUsers, int numberOfCountersPerUser, int numberOfRevisionsPerDocument,
            int numberOfOrders, int deleteUserFactor, DocumentStore storeToExport, string file, DocumentStore storeToImport, string fileAfterDeletions,
            DocumentStore storeToAfterDeletions)
        {
            if (numberOfRevisionsPerDocument > 0)
            {
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 } };

                await storeToExport.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));
            }

            using (var bulk = storeToExport.BulkInsert())
            {
                for (int i = 0; i < Math.Max(numberOfUsers, numberOfOrders); i++)
                {
                    if (i < numberOfUsers)
                        bulk.Store(new User(), "users/" + i);

                    if (i < numberOfOrders)
                        bulk.Store(new Order(), "orders/" + i);
                }
            }

            if (numberOfRevisionsPerDocument > 2)
            {
                for (int j = 0; j < numberOfRevisionsPerDocument; j++)
                {
                    using (var bulk = storeToExport.BulkInsert())
                    {
                        for (int i = 0; i < Math.Max(numberOfUsers, numberOfOrders); i++)
                        {
                            if (i < numberOfUsers)
                            {
                                bulk.Store(new User() { Name = i + " " + j }, "users/" + i);
                            }

                            if (i < numberOfOrders)
                            {
                                bulk.Store(new Order() { Company = i + " " + j }, "orders/" + i);
                            }
                        }
                    }
                }
            }

            using (var session = storeToExport.OpenSession())
            {
                for (int i = 0; i < numberOfUsers; i++)
                {
                    for (int j = 0; j < numberOfCountersPerUser; j++)
                    {
                        session.CountersFor("users/" + i).Increment("counter/" + j, 100);
                    }
                }

                session.SaveChanges();
            }

            var originalStats = await storeToExport.Maintenance.SendAsync(new GetStatisticsOperation());

            var options = new DatabaseSmugglerExportOptions();

            var operation = await storeToExport.Smuggler.ExportAsync(options, file);
            var result = await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            SmugglerResult.SmugglerProgress progress = ((SmugglerResult)result).Progress as SmugglerResult.SmugglerProgress;

            Assert.Equal(originalStats.CountOfDocuments, progress.Documents.ReadCount);
            Assert.Equal(originalStats.CountOfCounterEntries, progress.Counters.ReadCount);
            Assert.Equal(originalStats.CountOfRevisionDocuments, progress.RevisionDocuments.ReadCount);

            operation = await storeToImport.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            var stats = await storeToImport.Maintenance.SendAsync(new GetStatisticsOperation());

            Assert.Equal(numberOfUsers + numberOfOrders, stats.CountOfDocuments);
            Assert.Equal(numberOfUsers, stats.CountOfCounterEntries);

            var expectedNumberOfRevisions = (numberOfUsers + numberOfOrders) * numberOfRevisionsPerDocument;

            if (numberOfCountersPerUser > 0)
            {
                // if we added counters then additional revisions were created
                expectedNumberOfRevisions += numberOfUsers;
            }

            Assert.Equal(expectedNumberOfRevisions, stats.CountOfRevisionDocuments);

            // deleting some docs

            var deletedUsers = 0;

            using (var session = storeToExport.OpenSession())
            {
                for (int i = 0; i < numberOfUsers; i++)
                {
                    if (i % deleteUserFactor != 0)
                        continue;

                    session.Delete("users/" + i);

                    deletedUsers++;
                }

                session.SaveChanges();
            }

            // import to new db

            var originalStatsAfterDeletions = await storeToExport.Maintenance.SendAsync(new GetStatisticsOperation());

            options.OperateOnTypes |= DatabaseItemType.Tombstones;

            operation = await storeToExport.Smuggler.ExportAsync(options, fileAfterDeletions);
            result = await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            progress = ((SmugglerResult)result).Progress as SmugglerResult.SmugglerProgress;

            Assert.Equal(originalStatsAfterDeletions.CountOfDocuments, progress.Documents.ReadCount);
            Assert.Equal(originalStatsAfterDeletions.CountOfCounterEntries, progress.Counters.ReadCount);
            Assert.Equal(originalStatsAfterDeletions.CountOfRevisionDocuments, progress.RevisionDocuments.ReadCount);
            Assert.Equal(originalStatsAfterDeletions.CountOfTombstones, progress.Tombstones.ReadCount);

            var importOptions = new DatabaseSmugglerImportOptions();

            importOptions.OperateOnTypes |= DatabaseItemType.Tombstones;

            operation = await storeToAfterDeletions.Smuggler.ImportAsync(importOptions, fileAfterDeletions);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            var statsAfterDeletions = await storeToAfterDeletions.Maintenance.SendAsync(new GetStatisticsOperation());

            Assert.Equal(numberOfUsers - deletedUsers + numberOfOrders, statsAfterDeletions.CountOfDocuments);
            Assert.Equal(numberOfUsers - deletedUsers, statsAfterDeletions.CountOfCounterEntries);
            Assert.Equal(expectedNumberOfRevisions, statsAfterDeletions.CountOfRevisionDocuments);
            Assert.Equal(deletedUsers, statsAfterDeletions.CountOfTombstones);
        }

        protected static void CanStreamDocumentsWithPulsatingReadTransaction_ActualTest(int numberOfUsers, int numberOfOrders, int deleteUserFactor, DocumentStore store)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < Math.Max(numberOfUsers, numberOfOrders); i++)
                {
                    if (i < numberOfUsers)
                        bulk.Store(new User(), "users/" + i);

                    if (i < numberOfOrders)
                        bulk.Store(new Order(), "orders/" + i);
                }
            }

            AssertAllDocsStreamed(store, numberOfUsers, numberOfOrders);

            AssertAllDocsStreamedWithPaging(store, numberOfUsers, numberOfOrders);

            AssertAllStartsWithDocsStreamed(store, numberOfUsers);

            AssertAllStartsWithDocsStreamedWithPaging(store, numberOfUsers);

            AssertAllStartAfterDocsStreamed(store, numberOfUsers);

            AssertAllMatchesDocsStreamed(store, numberOfUsers);

            AssertAllMatchesDocsStreamedWithPaging(store, numberOfUsers);

            // deleting some docs

            var deletedUsers = 0;

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < numberOfUsers; i++)
                {
                    if (i % deleteUserFactor != 0)
                        continue;

                    session.Delete("users/" + i);

                    deletedUsers++;
                }

                session.SaveChanges();
            }

            AssertAllDocsStreamed(store, numberOfUsers - deletedUsers, numberOfOrders);

            AssertAllDocsStreamedWithPaging(store, numberOfUsers - deletedUsers, numberOfOrders);

            AssertAllStartsWithDocsStreamed(store, numberOfUsers - deletedUsers);

            AssertAllStartsWithDocsStreamedWithPaging(store, numberOfUsers - deletedUsers);

            AssertAllStartAfterDocsStreamed(store, numberOfUsers - deletedUsers);

            AssertAllMatchesDocsStreamed(store, numberOfUsers - deletedUsers);

            AssertAllMatchesDocsStreamed(store, numberOfUsers);

            AssertAllMatchesDocsStreamedWithPaging(store, numberOfUsers);
        }

        public class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users select new
                {
                    user.Name,
                    user.LastName
                };
            }
        }

        protected void CanStreamQueryWithPulsatingReadTransaction_ActualTest(int numberOfUsers, DocumentStore store)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < numberOfUsers; i++)
                {
                    bulk.Store(new User(), "users/" + i);
                }
            }

            new Users_ByName().Execute(store);

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var query = session.Query<User, Users_ByName>();

                var enumerator = session.Advanced.Stream<User>(query);

                var count = 0;

                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers, count);
            }
        }

        protected static void CanStreamCollectionQueryWithPulsatingReadTransaction_ActualTest(int numberOfUsers, DocumentStore store)
        {
            var uniqueUserNames = PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10;

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < numberOfUsers; i++)
                {
                    bulk.Store(new User()
                        {
                            Name = "users-" + (i % uniqueUserNames)
                        }, "users/" + i);
                }
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<User>();

                var enumerator = session.Advanced.Stream<User>(query);

                var count = 0;

                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers, count);
            }

            // distinct

            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().Select(x => new User
                {
                    Name = x.Name
                }).Distinct();

                var enumerator = session.Advanced.Stream(query);

                var count = 0;

                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(uniqueUserNames, count);
            }

            // paging

            using (var session = store.OpenSession())
            {
                var skip = 100;
                var take = PulsedEnumerationState<object>.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10;

                var query = session.Query<User>().Skip(skip).Take(take);

                var enumerator = session.Advanced.Stream<User>(query);

                var count = 0;

                while (enumerator.MoveNext())
                {
                    count++;
                }

                Assert.Equal(take, count);
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

        private static void AssertAllStartAfterDocsStreamed(DocumentStore store, int numberOfUsers)
        {
            var ids = new HashSet<string>();

            using (var session = store.OpenSession())
            {
                var en = session.Advanced.Stream<User>("users/", startAfter: "users/2");

                var count = 0;

                while (en.MoveNext())
                {
                    var added = ids.Add(en.Current.Id);

                    Assert.True(added, "Duplicated Id: " + en.Current.Id);

                    Assert.True(en.Current.Id.CompareTo("users/2") >= 0, "Found Id that isn't greater that startsAfter parameter: " + en.Current.Id);

                    count++;
                }

                Assert.True(count > 0, "count > 0");
                Assert.True(count < numberOfUsers, "count < numberOfUsers");
            }
        }

        private static void AssertAllMatchesDocsStreamed(DocumentStore store, int numberOfUsers)
        {
            var ids = new HashSet<string>();

            using (var session = store.OpenSession())
            {
                var en = session.Advanced.Stream<User>("u", matches: "*ers/2*");

                var count = 0;

                while (en.MoveNext())
                {
                    var added = ids.Add(en.Current.Id);

                    Assert.True(added, "Duplicated Id: " + en.Current.Id);

                    Assert.True(en.Current.Id.StartsWith("users/2"), "Found Id that doesn't start with 'users/2': " + en.Current.Id);

                    count++;
                }

                Assert.True(count > 0, "count > 0");
                Assert.True(count < numberOfUsers, "count < numberOfUsers");
            }
        }

        private static void AssertAllMatchesDocsStreamedWithPaging(DocumentStore store, int numberOfUsers)
        {
            var ids = new HashSet<string>();

            using (var session = store.OpenSession())
            {
                var matches = "*ers/2*";

                var en = session.Advanced.Stream<User>("u", matches: matches);

                var numberOfResults = 0;

                while (en.MoveNext())
                {
                    var added = ids.Add(en.Current.Id);

                    Assert.True(added, "Duplicated Id: " + en.Current.Id);

                    Assert.True(en.Current.Id.StartsWith("users/2"), "Found Id that doesn't start with 'users/2': " + en.Current.Id);

                    numberOfResults++;
                }

                Assert.True(numberOfResults > 0, "numberOfResults > 0");
                Assert.True(numberOfResults < numberOfUsers, "numberOfResults < numberOfUsers");

                var start = 10;

                en = session.Advanced.Stream<User>("u", start: start, matches: matches);

                int count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfResults - start, count);

                var take = numberOfResults / 2 + 3;

                en = session.Advanced.Stream<User>("u", start: start, matches: matches, pageSize: take);

                count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(take, count);
            }
        }

        private static void AssertAllDocsStreamedWithPaging(DocumentStore store, int numberOfUsers, int numberOfOrders)
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

                Assert.Equal(numberOfUsers + numberOfOrders - start, count);

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

        private static void AssertAllDocsStreamed(DocumentStore store, int numberOfUsers, int numberOfOrders)
        {
            using (var session = store.OpenSession())
            {
                var en = session.Advanced.Stream<User>((string)null);

                var count = 0;

                while (en.MoveNext())
                {
                    count++;
                }

                Assert.Equal(numberOfUsers + numberOfOrders, count);
            }
        }
    }
}
