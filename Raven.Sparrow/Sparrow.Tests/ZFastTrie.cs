using Sparrow.Binary;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Sparrow.Tests
{
    public class ZFastTrieTest
    {
        private readonly Func<string, BitVector> binarize = x => BitVector.Of(x, true);


        [Fact]
        public void Construction()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.Equal(0, tree.Count);
            Assert.Null(tree.FirstKeyOrDefault());
            Assert.Null(tree.LastKeyOrDefault());
        }


        [Fact]
        public void Operations_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));
            Assert.Equal(key, tree.FirstKey());
            Assert.Equal(key, tree.LastKey());
            Assert.True(tree.Contains(key));

            string value;
            Assert.True(tree.TryGet(key, out value));

            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(key, tree.SuccessorOrDefault(key));
            Assert.Null(tree.SuccessorOrDefault("qu"));

            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Null(tree.PredecessorOrDefault(key));
            Assert.Null(tree.PredecessorOrDefault("aq"));
            Assert.Equal(key, tree.PredecessorOrDefault("pq"));
        }

        [Fact]
        public void Structure_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));

            var successor = tree.SuccessorInternal(key);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.Null(successor.Previous.Key);
            Assert.Equal(tree.Head, successor.Previous);
            Assert.Equal(tree.Tail, successor.Next);

            Assert.Equal(key, successor.Key);

            var predecessor = tree.PredecessorInternal("yy");
            Assert.True(predecessor.IsLeaf);
            Assert.Null(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Equal(tree.Tail, predecessor.Next);
            Assert.Null(predecessor.Previous.Key);
            Assert.Equal(key, predecessor.Key);
                        
            Assert.Equal(predecessor, successor);
            Assert.Equal(tree.Root, predecessor);
        }

        [Fact]
        public void Operations_SingleBranchInsertion()
        {
            string smallestKey = "Ar";
            string lesserKey = "Oren";
            string greaterKey = "oren";
            string greatestKey = "zz";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.Equal(lesserKey, tree.FirstKey());
            Assert.Equal(greaterKey, tree.LastKey());

            Assert.True(tree.Contains(greaterKey));
            Assert.True(tree.Contains(lesserKey));

            string value;
            Assert.True(tree.TryGet(lesserKey, out value));
            Assert.True(tree.TryGet(greaterKey, out value));
            Assert.False(tree.TryGet(greaterKey + "1", out value));
            Assert.False(tree.TryGet("1", out value));

            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(lesserKey, tree.SuccessorOrDefault(lesserKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(greaterKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(lesserKey + "1"));
            Assert.Null(tree.SuccessorOrDefault(greatestKey));

            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Equal(lesserKey, tree.PredecessorOrDefault(greaterKey));
            Assert.Null(tree.PredecessorOrDefault(lesserKey));
            Assert.Null(tree.PredecessorOrDefault(smallestKey));
        }

        [Fact]
        public void Structure_SingleBranchInsertion()
        {
            string lesserKey = "Oren";
            string midKey = "aa";
            string greaterKey = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.True(tree.Root.IsInternal);

            var successor = tree.SuccessorInternal(midKey);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.NotNull(successor.Previous.Key);
            Assert.Equal(tree.Tail, successor.Next);

            var predecessor = tree.PredecessorInternal(midKey);
            Assert.True(predecessor.IsLeaf);
            Assert.NotNull(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Null(predecessor.Previous.Key);

            Assert.Equal(predecessor.Next, successor);
            Assert.Equal(successor.Previous, predecessor);
        }
    }
}
