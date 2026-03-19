using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public partial class RavenDB_25154
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldDetectConcurrencyViolations()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Egor", Age = 25 }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();
                var concurrencyExceptionThrown = new AsyncManualResetEvent();
                Exception caughtException = null;

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            foreach (var item in batch.Items)
                            {
                                var user = session.Load<User>(item.Id);
                                Assert.NotNull(user);
                            }

                            processedBatch.Set();

                            // Simulate concurrent modification
                            using (var backgroundSession = store.OpenSession())
                            {
                                var jerry = backgroundSession.Load<User>("users/1-A");
                                jerry.Age = 31;
                                backgroundSession.SaveChanges();
                            }

                            try
                            {
                                session.SaveChanges();
                            }
                            catch (Raven.Client.Exceptions.ConcurrencyException ex)
                            {
                                caughtException = ex;
                                concurrencyExceptionThrown.Set();
                                throw;
                            }
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                    Assert.True(await concurrencyExceptionThrown.WaitAsync(_reasonableWaitTime));
                    Assert.NotNull(caughtException);
                    Assert.Contains("Document 'users/1-A' has been modified", caughtException.Message);
                    Assert.Equal("users/1-A", ((Raven.Client.Exceptions.ConcurrencyException)caughtException).Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldNotThrowWhenNoModifications()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Egor", Age = 25 }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            foreach (var item in batch.Items)
                            {
                                var user = session.Load<User>(item.Id);
                                Assert.NotNull(user);
                            }

                            // Should not throw - entities are tracked but not modified
                            session.SaveChanges();
                            processedBatch.Set();
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldTrackIncludedDocuments()
        {
            using (var store = GetDocumentStore())
            {
                string addressId;
                using (var session = store.OpenSession())
                {
                    var address = new Address { City = "Harish", Street = "Erets Rd" };
                    session.Store(address);
                    addressId = session.Advanced.GetDocumentId(address);
                    address.Id = addressId;

                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Employees include Address.Id"
                });

                var processedBatch = new AsyncManualResetEvent();
                var concurrencyExceptionThrown = new AsyncManualResetEvent();
                Exception caughtException = null;

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<Employee>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            foreach (var item in batch.Items)
                            {
                                var employee = session.Load<Employee>(item.Id);
                                var address = session.Load<Address>(employee.Address.Id);
                                Assert.NotNull(address);
                                Assert.Equal(0, session.Advanced.NumberOfRequests); // Included, no request
                            }

                            processedBatch.Set();

                            // Modify included address in background
                            using (var backgroundSession = store.OpenSession())
                            {
                                var addr = backgroundSession.Load<Address>(addressId);
                                addr.City = "Hadera";
                                backgroundSession.SaveChanges();
                            }

                            try
                            {
                                session.SaveChanges();
                            }
                            catch (Raven.Client.Exceptions.ConcurrencyException ex)
                            {
                                caughtException = ex;
                                concurrencyExceptionThrown.Set();
                                throw;
                            }
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                    Assert.True(await concurrencyExceptionThrown.WaitAsync(_reasonableWaitTime));
                    Assert.NotNull(caughtException);
                    Assert.Contains($"Document '{addressId}' has been modified", caughtException.Message);
                    Assert.Equal(addressId, ((Raven.Client.Exceptions.ConcurrencyException)caughtException).Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldNotThrowAfterEvict()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Egor", Age = 25 }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            var jerry = session.Load<User>("users/1-A");
                            var egor = session.Load<User>("users/2-A");

                            // Evict Jerry
                            session.Advanced.Evict(jerry);

                            // Modify Jerry in background
                            using (var backgroundSession = store.OpenSession())
                            {
                                var j = backgroundSession.Load<User>("users/1-A");
                                j.Age = 31;
                                backgroundSession.SaveChanges();
                            }

                            // Should not throw - Jerry was evicted
                            session.SaveChanges();
                            processedBatch.Set();
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldUpdateChangeVectorAfterRefresh()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Egor", Age = 25 }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            var jerry = session.Load<User>("users/1-A");
                            var originalCv = session.Advanced.GetChangeVectorFor(jerry);

                            // Modify Jerry in background
                            using (var backgroundSession = store.OpenSession())
                            {
                                var j = backgroundSession.Load<User>("users/1-A");
                                j.Age = 31;
                                backgroundSession.SaveChanges();
                            }

                            // Refresh to get updated change vector
                            session.Advanced.Refresh(jerry);

                            var newCv = session.Advanced.GetChangeVectorFor(jerry);
                            Assert.NotEqual(originalCv, newCv);
                            Assert.Equal(31, jerry.Age);

                            // Should not throw - refresh updated the change vector
                            session.SaveChanges();
                            processedBatch.Set();
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldHandleMultipleBatches()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                // Create documents in multiple batches
                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = $"User{i}", Age = 20 + i }, $"users/{i}-A");
                        await session.SaveChangesAsync();
                    }
                }

                var batchesProcessed = 0;
                var allBatchesProcessed = new AsyncManualResetEvent();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 2
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            foreach (var item in batch.Items)
                            {
                                var user = session.Load<User>(item.Id);
                                Assert.NotNull(user);
                            }

                            session.SaveChanges();

                            if (Interlocked.Increment(ref batchesProcessed) >= 3)
                            {
                                allBatchesProcessed.Set();
                            }
                        }
                    });

                    Assert.True(await allBatchesProcessed.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldDetectDeletedEntityConcurrency()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Egor", Age = 25 }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();
                var concurrencyExceptionThrown = new AsyncManualResetEvent();
                Exception caughtException = null;

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            var jerry = session.Load<User>("users/1-A");
                            var egor = session.Load<User>("users/2-A");

                            // Mark Jerry for deletion
                            session.Delete(jerry);

                            processedBatch.Set();

                            // Modify Jerry in background before the delete is committed
                            using (var backgroundSession = store.OpenSession())
                            {
                                var j = backgroundSession.Load<User>("users/1-A");
                                j.Age = 31;
                                backgroundSession.SaveChanges();
                            }

                            try
                            {
                                session.SaveChanges();
                            }
                            catch (Raven.Client.Exceptions.ConcurrencyException ex)
                            {
                                caughtException = ex;
                                concurrencyExceptionThrown.Set();
                                throw;
                            }
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                    Assert.True(await concurrencyExceptionThrown.WaitAsync(_reasonableWaitTime));
                    Assert.NotNull(caughtException);
                    // The DELETE command detects the CV mismatch (background modified jerry)
                    Assert.Contains("users/1-A", caughtException.Message);
                    Assert.Equal("users/1-A", ((Raven.Client.Exceptions.ConcurrencyException)caughtException).Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_AndNoTracking_ShouldNotTrack()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        // Use NoTracking mode - should not track entities
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            NoTracking = true
                        }))
                        {
                            var jerry = session.Load<User>("users/1-A");
                            Assert.NotNull(jerry);

                            var inMemSession = (InMemoryDocumentSessionOperations)session;
                            Assert.False(inMemSession.TrackedEntities.TryGetValue("users/1-A", out _));

                            // Modify in background
                            using (var backgroundSession = store.OpenSession())
                            {
                                var j = backgroundSession.Load<User>("users/1-A");
                                j.Age = 31;
                                backgroundSession.SaveChanges();
                            }

                            // Should not throw - NoTracking mode
                            session.SaveChanges();
                            processedBatch.Set();
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task Subscription_WithTrackAllEntities_ShouldTrackMissingDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Users"
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jerry", Age = 30 }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                var processedBatch = new AsyncManualResetEvent();
                var concurrencyExceptionThrown = new AsyncManualResetEvent();
                Exception caughtException = null;

                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
                {
                    MaxDocsPerBatch = 10
                }))
                {
                    var t = subscription.Run(batch =>
                    {
                        using (var session = batch.OpenSession(new SessionOptions
                        {
                            OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                        }))
                        {
                            // Load a document that doesn't exist - should track as missing
                            var missingUser = session.Load<User>("users/missing-A");
                            Assert.Null(missingUser);

                            var inMemSession = (InMemoryDocumentSessionOperations)session;
                            Assert.True(inMemSession.TrackedEntities.TryGetValue("users/missing-A", out var cv));
                            Assert.Equal(string.Empty, cv);

                            processedBatch.Set();

                            // Create the missing document in background
                            using (var backgroundSession = store.OpenSession())
                            {
                                backgroundSession.Store(new User { Name = "Missing User", Age = 40 }, "users/missing-A");
                                backgroundSession.SaveChanges();
                            }

                            try
                            {
                                session.SaveChanges();
                            }
                            catch (Raven.Client.Exceptions.ConcurrencyException ex)
                            {
                                caughtException = ex;
                                concurrencyExceptionThrown.Set();
                                throw;
                            }
                        }
                    });

                    Assert.True(await processedBatch.WaitAsync(_reasonableWaitTime));
                    Assert.True(await concurrencyExceptionThrown.WaitAsync(_reasonableWaitTime));
                    Assert.NotNull(caughtException);
                    Assert.Contains($"Document 'users/missing-A' has been modified", caughtException.Message);
                    Assert.Equal("users/missing-A", ((Raven.Client.Exceptions.ConcurrencyException)caughtException).Id);
                }
            }
        }
    }
}
