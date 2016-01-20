// -----------------------------------------------------------------------
//  <copyright file="TrieTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNet.Routing.Constraints;
using Raven.Server.Routing;
using Xunit;

namespace BlittableTests.Routing
{
    public class TrieTests
    {
        [Fact]
        public void CanQueryTrie()
        {
            var trie = Trie<int>.Build(new[]
            {
                "admin/databases",
                "databases/*/docs",
                "databases/*/queries",
                "fs/*/files",
                "admin/debug-info",
            }.ToDictionary(x => x, x => 1));

            int value;
            Assert.True(trie.TryGetValue("admin/databases", out value));
        }

        [Fact]
        public void CanBuildTrie()
        {
            var trie = Trie<int>.Build(new[]
            {
                "admin/databases",
                "databases/*/docs",
                "databases/*/queries",
                "fs/*/files",
                "admin/debug-info",
            }.ToDictionary(x => x, x => 1));

            Assert.Equal("", trie.Key);
            Assert.Equal(3, trie.Children.Length);

            Assert.Equal("admin/", trie.Children[0].Key);
            Assert.Equal(2, trie.Children[0].Children.Length);
            Assert.Equal("admin/", trie.Children[0].Children[0].Key);
            Assert.Equal("admin/", trie.Children[0].Children[0].Key);
            Assert.Equal("databases/*/docs", trie.Children[1].Key);
            Assert.Equal("fs/*/files", trie.Children[2].Key);
        } 
    }
}