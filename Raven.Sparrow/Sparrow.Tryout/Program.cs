using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Tests;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Tryout
{
    class Program
    {
        private static readonly Func<string, BitVector> binarize = x => BitVector.Of(x, true);

        static unsafe void Main(string[] args)
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            tree.Add("8Jp3", "8Jp3");
            tree.Add("GX37", "GX37");
            tree.Add("f04o", "f04o");
            tree.Add("KmGx", "KmGx");

            // ZFastTrieDebugHelpers.StructuralVerify(tree);

            ZFastTrieDebugHelpers.DumpTree(tree);
        }
    }
}
