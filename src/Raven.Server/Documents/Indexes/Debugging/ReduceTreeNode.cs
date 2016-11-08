using System.Collections.Generic;
using Sparrow.Json;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public class ReduceTreeNode
    {
        public ReduceTreeNode()
        {
        }

        public ReduceTreeNode(int numberOfChildren)
        {
            Children = new List<ReduceTreeNode>(numberOfChildren);
        }

        public ReduceTreeNode(TreePage p)
        {
            Page = p;
            Children = new List<ReduceTreeNode>(p.NumberOfEntries);
        }

        public readonly TreePage Page;

        public readonly List<ReduceTreeNode> Children;

        public BlittableJsonReaderObject Data;

        public string Name;
    }
}