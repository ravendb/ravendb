// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3145.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3145 : RavenTestBase
    {
        public RavenDB_3145(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var result1 = commands.Put("key/1", null, new { });
                    var result2 = commands.Put("key/1", null, new { });

                    var e = Assert.Throws<ConcurrencyException>(() => commands.Delete("key/1", result1.ChangeVector));
                    Assert.Contains("Optimistic concurrency violation, transaction will be aborted.", e.Message);
                }
            }
        }
    }
}
