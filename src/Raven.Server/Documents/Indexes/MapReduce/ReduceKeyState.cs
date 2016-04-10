using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class ReduceKeyState
    {
        public readonly Tree Tree;
        public readonly HashSet<long> ModifiedPages = new HashSet<long>();
        public readonly HashSet<long> FreedPages = new HashSet<long>();

        public ReduceKeyState(Tree tree)
        {
            Tree = tree;
            Tree.PageModified += page => ModifiedPages.Add(page);
            Tree.PageFreed += page => FreedPages.Add(page);
        }
    }
}