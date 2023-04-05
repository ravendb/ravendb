using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Voron;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.CompactTrees
{
    public class CompactTreeIteratorTest : StorageTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public CompactTreeIteratorTest(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
        {
            _testOutputHelper = testOutputHelper;
        }

        public static IEnumerable<object[]> Configuration =>
            new List<object[]>
            {
                new object[] { Random.Shared.Next(100000), Random.Shared.Next()},
                new object[] { Random.Shared.Next(10000), Random.Shared.Next(), },
                new object[] { Random.Shared.Next(1000), Random.Shared.Next()},
                new object[] { Random.Shared.Next(100), Random.Shared.Next()},
            };


        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1, 1337)]
        [InlineData(380, 1158997728)]
        [MemberData("Configuration")]
        public void IterateAndCompare(int itemsToInsert, int randomSeed = 1337)
        {
            var currentKeys = new List<string>();
            var rnd = new Random(randomSeed);

            // Initializing the tree with random writes. Depending on the tree size, it may
            // cause the creation of new dictionaries and transitioning pages. 
            using (var wtx = Env.WriteTransaction())
            {
                var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                for (int j = 0; j < itemsToInsert; j++)
                {
                    long item = Math.Abs((long)rnd.Next() + rnd.Next());
                    var itemAsString = item.ToString();
                    currentKeys.Add(itemAsString);
                    tree.Add(itemAsString, item);
                }

                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                var forwardIterator = tree.Iterate();
                currentKeys.Sort();

                int i = 0;
                forwardIterator.Reset();
                while (forwardIterator.MoveNext(out var scoped, out long value))
                {
                    Assert.Equal(value, long.Parse(currentKeys[i]));
                    scoped.Dispose();
                    i++;
                }
                Assert.Equal(currentKeys.Count, i);

                var backwardIterator = tree.Iterate<CompactTree.BackwardIterator>();
                currentKeys.Reverse();

                i = 0;
                backwardIterator.Reset();
                while (backwardIterator.MoveNext(out var scoped, out long value))
                {
                    Assert.Equal(value, long.Parse(currentKeys[i]));
                    scoped.Dispose();
                    i++;
                }

                Assert.Equal(currentKeys.Count, i);
            }
        }
    }
}
