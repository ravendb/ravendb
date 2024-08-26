using System;
using System.Collections.Generic;
using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class TreeRebalancer : FastTests.Voron.StorageTest
    {
        public TreeRebalancer(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TreeRabalancerShouldCopyNodeFlagsWhenMultiValuePageRefIsSet()
        {
            var addedIds = new Dictionary<string, string>();

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()))
            {
                var multiTrees = CreateTrees(env, 1, "multiTree");
                using (var tx = env.WriteTransaction())
                {
                    for (var i = 0; i < 120; i++)
                    {
                        foreach (var multiTreeName in multiTrees)
                        {
                            var multiTree = tx.CreateTree(multiTreeName);
                            var id = Guid.NewGuid().ToString();

                            addedIds.Add("test/0/user-" + i, id);

                            multiTree.MultiAdd("test/0/user-" + i, id);
                        }
                    }

                    foreach (var multiTreeName in multiTrees)
                    {
                        var multiTree = tx.CreateTree(multiTreeName);
                        multiTree.MultiAdd("test/0/user-50", Guid.NewGuid().ToString());
                    }

                    tx.Commit();
                }


                for (var i = 119; i > 99; i--)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        foreach (var multiTreeName in multiTrees)
                        {
                            var multiTree = tx.CreateTree(multiTreeName);
                    
                            multiTree.MultiDelete("test/0/user-" + i, addedIds["test/0/user-" + i]);
                        }

                        tx.Commit();
                    }

                    ValidateMulti(env, multiTrees);
                }

                for (var i = 0; i < 50; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                

                        foreach (var multiTreeName in multiTrees)
                        {
                            var multiTree = tx.CreateTree(multiTreeName);
                    
                            multiTree.MultiDelete("test/0/user-" + i, addedIds["test/0/user-" + i]);
                        }

                        tx.Commit();
                    }

                    ValidateMulti(env, multiTrees);
                }

                ValidateMulti(env, multiTrees);
            }
        }

        private void ValidateMulti(StorageEnvironment env, IEnumerable<string> trees)
        {
            using (var snapshot = env.ReadTransaction())
            {
                foreach (var tree in trees)
                {
                    using (var iterator = snapshot.ReadTree(tree).MultiRead( "test/0/user-50"))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                        var keys = new HashSet<string>();

                        var count = 0;
                        do
                        {
                            keys.Add(iterator.CurrentKey.ToString());
                            Guid.Parse(iterator.CurrentKey.ToString());

                            count++;
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(2, count);
                        Assert.Equal(2, keys.Count);
                    }
                }
            }
        }

        [Fact]
        public void ShouldNotThrowThatPageIsFullDuringTreeRebalancing()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree( "rebalancing-issue");

                var aKey = new string('a', 1024);
                var bKey = new string('b', 1024);
                var cKey = new string('c', 1024);
                var dKey = new string('d', 1024);
                var eKey = new string('e', 600);
                var fKey = new string('f', 920);

                tree.Add(aKey, new MemoryStream(new byte[1000]));
                tree.Add(bKey, new MemoryStream(new byte[1000]));
                tree.Add(cKey, new MemoryStream(new byte[1000]));
                tree.Add(dKey, new MemoryStream(new byte[1000]));
                tree.Add(eKey, new MemoryStream(new byte[800]));
                tree.Add(fKey, new MemoryStream(new byte[10]));

                // to expose the bug we need to delete the last item from the left most page
                // tree rebalance will try to fix the first reference (the implicit ref page node) in the parent page which is almost full 
                // and will fail because there is no space to put a new node

                tree.Delete(aKey); // this line throws "The page is full and cannot add an entry, this is probably a bug"


                using (var iterator = tree.Iterate(false))
                {
                    Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                    Assert.Equal(bKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(cKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(dKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(eKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(fKey, iterator.CurrentKey.ToString());
                    Assert.False(iterator.MoveNext());
                }

                tx.Commit();
            }
        }

        [Fact]
        public void RavenDB_2543_CouldNotEnsureThatWeHaveEnoughSpace_When_MovingLeafNode()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree( "rebalancing-issue");

                var aKey = new string('a', 1000);
                var bKey = new string('b', 1000);
                var cKey = new string('c', 1000);
                var dKey = new string('d', 1020);
                var eKey = new string('e', 1020);
                var fKey = new string('f', 1020);
                var gKey = new string('g', 1000);
                var hKey = new string('h', 1000);
                var iKey = new string('i', 1000);
                var jKey = new string('j', 1000);
                var kKey = new string('k', 1000);
                var lKey = new string('l', 1000);
                var mKey = new string('m', 820);
                var nKey = new string('n', 102);

                tree.Add(aKey, new MemoryStream(new byte[100]));
                tree.Add(bKey, new MemoryStream(new byte[100]));
                tree.Add(cKey, new MemoryStream(new byte[100]));
                tree.Add(dKey, new MemoryStream(new byte[100]));
                tree.Add(eKey, new MemoryStream(new byte[100]));
                tree.Add(fKey, new MemoryStream(new byte[100]));
                tree.Add(gKey, new MemoryStream(new byte[100]));
                tree.Add(hKey, new MemoryStream(new byte[100]));
                tree.Add(iKey, new MemoryStream(new byte[100]));
                tree.Add(jKey, new MemoryStream(new byte[100]));
                tree.Add(kKey, new MemoryStream(new byte[100]));
                tree.Add(lKey, new MemoryStream(new byte[100]));
                tree.Add(mKey, new MemoryStream(new byte[100]));
                tree.Add(nKey, new MemoryStream(new byte[1000]));



                tree.Delete(nKey);  // this line throws "The page is full and cannot add an entry, this is probably a bug"

                using (var iterator = tree.Iterate(false))
                {
                    Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                    Assert.Equal(aKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(bKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(cKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(dKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(eKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(fKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(gKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(hKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(iKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(jKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(kKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(lKey, iterator.CurrentKey.ToString());
                    Assert.True(iterator.MoveNext());

                    Assert.Equal(mKey, iterator.CurrentKey.ToString());
                    Assert.False(iterator.MoveNext());
                }

                tx.Commit();
            }
        }
    }
}
