// -----------------------------------------------------------------------
//  <copyright file="TrieTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Server.Routing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Routing
{
    public class TrieTests : NoDisposalNeeded
    {
        public TrieTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryTrie()
        {
            var trie = Trie<int>.Build(new[]
            {
                "/admin/databases",
                "/databases/*/docs",
                "/databases/*/queries",
                "/fs/*/files",
                "/admin/debug-info",
            }.ToDictionary(x => "GET" + x, x => 1));

            Assert.True(trie.TryMatch("GET", "/admin/databases").Value == 1);
        }


        [Fact]
        public void CanQueryWithRoot()
        {
            var trie = Trie<int>.Build(new[]
            {
                "/",
                "/build/version",
                "/databases",
                "/databases/*/docs",
                "/databases/*/indexes",
            }.ToDictionary(x => "GET" + x, x => 1));

            Assert.True(trie.TryMatch("GET", "/build/version").Value == 1);
        }


        [Theory]
        [InlineData("databases/northwind/docs")]
        [InlineData("databases")]
        [InlineData("databases/northwind/indexes/Raven/DocumentsByEntityName")]
        [InlineData("Databases/northwind/Docs")]
        [InlineData("Databases/רוח-צפונית/Docs")]
        public void CanQueryTrieWithParams(string url)
        {
            // /databases/northwind/indexes/Raven/DocumentsByEntityName
            var trie = Trie<int>.Build(new[]
            {
                "admin/databases",
                "databases/*/docs",
                "databases",
                "databases/*/queries",
                "databases/*/indexes/$",
                "fs/*/files",
                "admin/debug-info",
            }.ToDictionary(x => "GET" + x, x => 1));

            Assert.True(trie.TryMatch("GET", url).Value == 1);
        }
    }
}
