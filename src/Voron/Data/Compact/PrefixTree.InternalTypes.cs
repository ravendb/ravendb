using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    partial class PrefixTree
    {
        /// <summary>
        /// The cutpoint for a string x with respect to the trie is the length of the longest common prefix between x and exit(x).
        /// </summary>
        private sealed unsafe class CutPoint
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The parent of the exit node.
            /// </summary>
            public readonly Internal* Parent;

            /// <summary>
            /// The binary representation of the search key.
            /// </summary>
            public readonly BitVector SearchKey;

            /// <summary>
            /// The exit node. If parex(x) == root then exit(x) is the root; otherwise, exit(x) is the left or right child of parex(x) 
            /// depending whether x[|e-parex(x)|] is zero or one, respectively. Page 166 of [1]
            /// </summary>
            public readonly Node* Exit;

            public CutPoint(int lcp, Internal* parent, Node* exit, BitVector searchKey)
            {
                this.LongestPrefix = lcp;
                this.Parent = parent;
                this.Exit = exit;
                this.SearchKey = searchKey;
            }

            public bool IsCutLow(PrefixTree owner)
            {
                return owner.IsCutLow(this.Exit, this.LongestPrefix);
            }

            public bool IsRightChild
            {
                get { return this.Parent != null && this.Parent == this.Exit; }
            }
        }

        private sealed unsafe class ExitNode
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The exit node, it will be a leaf when the search key matches the query. 
            /// </summary>
            public readonly Node* Exit;

            public readonly BitVector SearchKey;

            public ExitNode(int lcp, Node* exit, BitVector v)
            {
                this.LongestPrefix = lcp;
                this.Exit = exit;
                this.SearchKey = v;
            }
        }
    }
}
