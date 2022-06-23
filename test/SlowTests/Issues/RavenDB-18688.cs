using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18688 : RavenTestBase
    {
        public RavenDB_18688(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Doesnt_Throw_When_Index_Is_Disabled()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                await index.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation(index.IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Name = "Grisha"
                        });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Name = "Grisha"
                        });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), indexes: new[] { index.IndexName });
                    var error = await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains("could not verify that all indexes has caught up with the changes as of etag", error.Message);
                    Assert.Contains("Total relevant indexes: 1, total stale indexes: 1", error.Message);
                    Assert.DoesNotContain("total paused indexes", error.Message);
                }
            }
        }

        [Fact]
        public async Task Doesnt_Throw_When_Index_Is_Errored()
        {
            using (var store = GetDocumentStore())
            {
                var index = new ErroredIndex();
                await index.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Count = 0
                        });
                    await session.SaveChangesAsync();
                }

                var state = await WaitForValueAsync(async () =>
                {
                    var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));
                    return indexStats.State;
                }, IndexState.Error);

                Assert.Equal(IndexState.Error, state);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Count = 0
                        });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                    var error = await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains($"Total relevant indexes: 1, total stale indexes: 1, total errored indexes: 1 ({index.IndexName})", error.Message);
                    Assert.DoesNotContain("total paused indexes", error.Message);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Count = 0
                        });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), indexes: new[] { index.IndexName });
                    var error = await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains($"Total relevant indexes: 1, total stale indexes: 1, total errored indexes: 1 ({index.IndexName})", error.Message);
                    Assert.DoesNotContain("total paused indexes", error.Message);
                }

                await new Index().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new User
                        {
                            Count = 0
                        });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5));
                    var error = await Assert.ThrowsAsync<RavenTimeoutException>(async () => await session.SaveChangesAsync());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains($"Total relevant indexes: 2, total stale indexes: 1, total errored indexes: 1 ({index.IndexName})", error.Message);
                    Assert.DoesNotContain("total paused indexes", error.Message);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        private class ErroredIndex : AbstractIndexCreationTask<User>
        {
            public ErroredIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   Count = 3 / user.Count
                               };
            }
        }
    }
}
