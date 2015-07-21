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
        [Fact]
        public void Construction()
        {
            var tree = new ZFastTrieSortedSet<string, string>(x => BitVector.Of(x));
            Assert.Equal(0, tree.Count);
            Assert.Null(tree.FirstKeyOrDefault());
            Assert.Null(tree.LastKeyOrDefault());
        }


        [Fact]
        public void SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(x => BitVector.Of(x));
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
        public void SingleBranchInsertion()
        {
            var tree = new ZFastTrieSortedSet<string, string>(x => BitVector.Of(x));
            Assert.True(tree.Add("oren", "eini"));
            Assert.True(tree.Add("Oren", "Eini"));

            Assert.Equal("oren", tree.FirstKey());
            Assert.Equal("Oren", tree.LastKey());

            Assert.True(tree.Contains("Oren"));
            Assert.True(tree.Contains("oren"));

            string value;
            Assert.True(tree.TryGet("oren", out value));
            Assert.True(tree.TryGet("Oren", out value));
            Assert.False(tree.TryGet("Oren1", out value));
            Assert.False(tree.TryGet("1", out value));


            Assert.Equal("Oren", tree.SuccessorOrDefault("oren"));
            Assert.Equal("oren", tree.PredecessorOrDefault("Oren"));

            Assert.Null(tree.SuccessorOrDefault("Oren"));
            Assert.Null(tree.PredecessorOrDefault("oren"));
        }
    }
}
