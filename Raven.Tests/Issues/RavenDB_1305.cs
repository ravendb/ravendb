// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1305.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1305 : RavenTest
    {
        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void BulkInsertOperationShouldNotThrowConcurrencyExceptionOnDuplicateKeysWhenCheckForUpdatesIsEnabled(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Put("foos/1", null, new RavenJObject(), new RavenJObject());

                Assert.DoesNotThrow(
                    () =>
                    {
                        using (var bulk = store.BulkInsert(options: new BulkInsertOptions
                                                                    {
                                                                        CheckForUpdates = true
                                                                    }))
                        {
                            bulk.Store(new { }, "foos/1");
                        }
                    });
            }

            using (var store = NewDocumentStore(requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Put("foos/1", null, new RavenJObject(), new RavenJObject());

                Assert.DoesNotThrow(
                    () =>
                    {
                        using (var bulk = store.BulkInsert(options: new BulkInsertOptions
                        {
                            CheckForUpdates = true
                        }))
                        {
                            bulk.Store(new { }, "foos/1");
                        }
                    });
            }
        }

        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void BulkInsertOperationShouldThrowConcurrencyExceptionOnDuplicateKeysRemote(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Put("foos/1", null, new RavenJObject(), new RavenJObject());

                var e = Assert.Throws<ConcurrencyException>(
                    () =>
                    {
                        using (var bulk = store.BulkInsert())
                        {
                            bulk.Store(new { }, "foos/1");
                        }
                    });

                Assert.Equal("Illegal duplicate key foos/1", e.Message);
            }
        }

        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void BulkInsertOperationShouldThrowConcurrencyExceptionOnDuplicateKeys(string requestedStorage)
        {
            using (var store = NewDocumentStore(requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Put("foos/1", null, new RavenJObject(), new RavenJObject());

                Assert.Throws<ConcurrencyException>(
                    () =>
                    {
                        using (var bulk = store.BulkInsert())
                        {
                            bulk.Store(new { }, "foos/1");
                        }
                    });
            }
        }
    }
}