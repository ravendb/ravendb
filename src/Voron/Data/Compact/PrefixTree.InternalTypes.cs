using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            public readonly short LongestPrefix;

            /// <summary>
            /// The parent of the exit node 
            /// <see cref="Internal*"/>
            /// </summary>
            public readonly long Parent;

            /// <summary>
            /// The binary representation of the search key.
            /// </summary>
            public readonly BitVector SearchKey;

            /// <summary>
            /// The exit node. If parex(x) == root then exit(x) is the root; otherwise, exit(x) is the left or right child of parex(x) 
            /// depending whether x[|e-parex(x)|] is zero or one, respectively. Page 166 of [1] 
            /// <see cref="Node*"/>
            /// </summary>
            public readonly long Exit;

            public readonly bool IsRightChild;

            public CutPoint(int lcp, long parent, long exit, long parentRight, BitVector searchKey)
            {
                Debug.Assert(lcp < short.MaxValue);

                this.LongestPrefix = (short)lcp;
                this.Parent = parent;
                this.Exit = exit;
                this.SearchKey = searchKey;
                this.IsRightChild = parent != Constants.InvalidNodeName && parentRight == exit;
            }

            public bool IsCutLow(PrefixTree owner)
            {
                var exitNode = owner.ReadNodeByName(this.Exit);
                return owner.IsCutLow(exitNode, this.LongestPrefix);
            }
        }

        private sealed unsafe class ExitNode
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The exit node name, it will be a leaf when the search key matches the query. 
            /// </summary>
            public readonly long Exit;

            public readonly BitVector SearchKey;

            public ExitNode(int lcp, long exit, BitVector v)
            {
                this.LongestPrefix = lcp;
                this.Exit = exit;
                this.SearchKey = v;
            }
        }
    }
}
