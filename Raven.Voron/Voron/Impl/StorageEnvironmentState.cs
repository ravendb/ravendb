using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Voron.Trees;
using System.Linq;

namespace Voron
{
    public class StorageEnvironmentState
    {
        private ImmutableDictionary<string, Tree> _trees =
            ImmutableDictionary<string, Tree>.Empty.WithComparers(StringComparer.OrdinalIgnoreCase);

        public ImmutableDictionary<string, Tree> Trees
        {
            get { return _trees; }
        }

        public Tree Root { get; set; }
        public Tree FreeSpaceRoot { get; set; }

        public long NextPageNumber;

        public StorageEnvironmentState() { }

        public StorageEnvironmentState(Tree freeSpaceRoot, Tree root, long nextPageNumber)
        {
            FreeSpaceRoot = freeSpaceRoot;
            Root = root;
            NextPageNumber = nextPageNumber;
        }

        public StorageEnvironmentState Clone()
        {
            return new StorageEnvironmentState()
                {
                    _trees = ImmutableDictionary<string, Tree>.Empty
                        .WithComparers(StringComparer.OrdinalIgnoreCase)
                        .AddRange(_trees.Select(x => new KeyValuePair<string, Tree>(x.Key, x.Value.Clone()))),
                    Root = Root != null ? Root.Clone() : null,
                    FreeSpaceRoot = FreeSpaceRoot != null ? FreeSpaceRoot.Clone() : null,
                    NextPageNumber = NextPageNumber
                };
        }

        public void RemoveTree(string name)
        {
            _trees = _trees.Remove(name);
        }

        public void AddTree(string name, Tree tree)
        {
            _trees = _trees.Add(name, tree);
        }
    }
}