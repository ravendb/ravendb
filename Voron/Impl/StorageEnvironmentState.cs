using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Trees;
using Voron.Util;

namespace Voron
{
    public class StorageEnvironmentState
    {
        private SafeDictionary<string, Tree> _trees =
			SafeDictionary<string, Tree>.Empty.WithComparers(StringComparer.OrdinalIgnoreCase);

		public SafeDictionary<string, Tree> Trees
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
                    _trees = SafeDictionary<string, Tree>.Empty
                        .WithComparers(StringComparer.OrdinalIgnoreCase)
                        .SetItems(_trees.ToDictionary(x=>x.Key, x=>x.Value.Clone())),
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