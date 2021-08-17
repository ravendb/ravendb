using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17087 : RavenTestBase
    {
        public RavenDB_17087(ITestOutputHelper output) : base(output)
        {
        }
        
        
        [Fact]
        public void CreateIndexWithKeyValueUsingLocalTypes()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new NavigationContext());
            }
        }
        
        
        private class NavigationContext : AbstractIndexCreationTask<Trie, NavigationContext.Result>
        {
            public class Result
            {
                public string Path { get; set; }
                public string Name { get; set; }
                public string Parent { get; set; }
                public IEnumerable<string> Children { get; set; }
                public IEnumerable<string> Ancestors { get; set; }
            }
            public NavigationContext()
            {
                Map = tries => from trie in tries
                    from node in trie.Nodes
                    let parentKey = node.Key.Remove(0, Math.Min(node.Key.LastIndexOf('/'), node.Key.Length))
                    let parent = trie.Nodes[parentKey].Id ?? ""
                    let children = ""
                    let ancestors = 
                        from ancestor in Recurse(node, n => LoadDocument<Trie>("Trie/Global").Nodes.Where(x => x.Key.Remove(0, Math.Min(x.Key.LastIndexOf('/'), x.Key.Length)) == n.Key))
                        select ancestor.Value.Id
                    select new
                    {
                        Path = node.Key,
                        Name = node.Value.Name,
                        Parent = parentKey,
                        Children = children,
                        Ancestors = ancestors
                    };
                Stores.Add(x => x.Name, FieldStorage.Yes);
                Stores.Add(x => x.Path, FieldStorage.Yes);
                Stores.Add(x => x.Parent, FieldStorage.Yes);
                Stores.Add(x => x.Children, FieldStorage.Yes);
                Stores.Add(x => x.Ancestors, FieldStorage.Yes);
            }
        }
        private class Trie
        {
            public string Id { get; set; } = string.Empty;
            public Dictionary<string, TrieNode> Nodes { get; set; } = new Dictionary<string, TrieNode>();
        }
        private class TrieNode
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
