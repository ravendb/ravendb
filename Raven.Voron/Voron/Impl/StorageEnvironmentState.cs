using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Impl;
using Voron.Trees;
using Voron.Util;

namespace Voron
{
    public class StorageEnvironmentState
    {
        private Dictionary<string, Tree> _trees = new Dictionary<string, Tree>(StringComparer.OrdinalIgnoreCase);

		public Dictionary<string, Tree> Trees
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
                    _trees = _trees.ToDictionary(x => x.Key, x => x.Value.Clone(), StringComparer.OrdinalIgnoreCase),
                    Root = Root != null ? Root.Clone() : null,
                    FreeSpaceRoot = FreeSpaceRoot != null ? FreeSpaceRoot.Clone() : null,
                    NextPageNumber = NextPageNumber
                };
        }

        public void RemoveTree(string name)
        {
            _trees.Remove(name);
        }

        public void AddTree(string name, Tree tree)
        {
            _trees.Add(name, tree);
        }
    }
}