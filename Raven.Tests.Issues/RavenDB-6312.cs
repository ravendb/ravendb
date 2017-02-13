// -----------------------------------------------------------------------
//  <copyright file="RavenDB-6312.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6312 : RavenTest
    {
        [Fact]
        public void HiLoKeyGenerator_works_without_aggressive_caching()
        {
            using (var store = NewDocumentStore())
            {
                var hilo = new HiLoKeyGenerator("test", 1);
                Assert.Equal(1L, hilo.NextId(store.DatabaseCommands));
                Assert.Equal(2L, hilo.NextId(store.DatabaseCommands));
            }
        }

        [Fact]
        public void HiLoKeyGenerator_hangs_when_aggressive_caching_enabled()
        {
            using (var store = NewDocumentStore())
            {
                using (store.AggressivelyCache())
                {
                    var hilo = new HiLoKeyGenerator("test", 1);
                    Assert.Equal(1L, hilo.NextId(store.DatabaseCommands));
                    Assert.Equal(2L, hilo.NextId(store.DatabaseCommands));
                }
            }
        }

        [Fact]
        public async Task HiLoKeyGenerator_async_hangs_when_aggressive_caching_enabled()
        {
            using (var store = NewDocumentStore())
            {
                using (store.AggressivelyCache())
                {
                    var hilo = new AsyncHiLoKeyGenerator("test", 1);
                    Assert.Equal(1L, await hilo.NextIdAsync(store.AsyncDatabaseCommands));
                    Assert.Equal(2L, await hilo.NextIdAsync(store.AsyncDatabaseCommands));
                }
            }
        }

        [Fact]
        public void HiLoKeyGenerator_hangs_when_aggressive_caching_enabled_on_other_documentstore()
        {
            using (var server = GetNewServer(port: 8079))
            using (var otherServer = GetNewServer(port: 8078))
            using (var store = NewRemoteDocumentStore(ravenDbServer: server))
            using (var otherStore = NewRemoteDocumentStore(ravenDbServer: otherServer))
            {
                using (otherStore.AggressivelyCache()) // Note that we don't even use the other store, we just call AggressivelyCache on it
                {
                    var hilo = new HiLoKeyGenerator("test", 1);
                    Assert.Equal(1L, hilo.NextId(store.DatabaseCommands));
                    Assert.Equal(2L, hilo.NextId(store.DatabaseCommands));
                }
            }
        }

        [Fact]
        public async Task HiLoKeyGenerator_async_hangs_when_aggressive_caching_enabled_on_other_documentstore()
        {
            using (var store = NewDocumentStore())
            using (var otherStore = NewDocumentStore())
            {
                using (otherStore.AggressivelyCache()) // Note that we don't even use the other store, we just call AggressivelyCache on it
                {
                    var hilo = new AsyncHiLoKeyGenerator("test", 1);
                    Assert.Equal(1L, await hilo.NextIdAsync(store.AsyncDatabaseCommands));
                    Assert.Equal(2L, await hilo.NextIdAsync(store.AsyncDatabaseCommands));
                }
            }
        }
    }
}