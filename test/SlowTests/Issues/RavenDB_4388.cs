// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4388.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4388 : RavenTestBase
    {
        public RavenDB_4388(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldCleanupCache()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("keys/1", null, new { });
                    commands.Get("keys/1");

                    Thread.Sleep(1);
                    var cache = commands.RequestExecutor.Cache;
                    Assert.True(cache.NumberOfItems > 0);
                    cache.Clear();
                    Assert.Equal(0, cache.NumberOfItems);
                }
            }
        }
    }
}
