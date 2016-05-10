// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4388.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Threading;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4388 : RavenTest
    {
        [Fact]
        public void ShouldCleanupCache()
        {
            ServicePointManager.MaxServicePointIdleTime = 1;
            using (var store = NewRemoteDocumentStore())
            {
                store.DatabaseCommands.Get("keys/1");
                Thread.Sleep(1);
                var cache = store.JsonRequestFactory.HttpClientCache;
                Assert.True(cache.Count > 0);
                cache.Cleanup(null);
                Assert.Equal(0, cache.Count);
            }
        }
    }
}