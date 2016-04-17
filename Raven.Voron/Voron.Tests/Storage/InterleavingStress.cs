using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Voron.Impl;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Storage
{
    public class InterleavingStress : StorageTest
    {
        [PrefixesFact]
        public void ShouldReusePagesButStillNotMixStuffUp()
        {
            var random = new Random(1234);
            var buffer = new byte[1024 * 512];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "ATree");
                Env.CreateTree(tx, "BTree");
                tx.Commit();
            }

            var generator = new Random(1000);

            var bTreeValues = new SortedDictionary<int, int>();

            // Starting the tree.
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var aTree = tx.Environment.CreateTree(tx, "ATree");
                var bTree = tx.Environment.CreateTree(tx, "BTree");

                for (int i = 0; i < 10000; i++)
                {
                    aTree.Add("A" + generator.Next(), new byte[0]);

                    int bKey = generator.Next();
                    bTree.Add("B" + bKey, new byte[0]);
                    bTreeValues[bKey] = bKey;
                }

                tx.Commit();
            }

            for ( int iterations = 0; iterations < 1000; iterations++ )
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var aTree = tx.Environment.CreateTree(tx, "ATree");
                    var bTree = tx.Environment.CreateTree(tx, "BTree");

                    if (generator.Next(3) == 0)
                    {
                        // The rare operation is to delete.     

                        // We dont have enough to actually make sense to delete. 
                        if (bTreeValues.Count < 200)
                            continue;

                        // We always delete from B
                        int i = generator.Next(bTreeValues.Count - 100);
                        
                        var valuesToIterate = new List<int>(bTreeValues.Keys.Skip(i).Take(100));
                        foreach ( int value in valuesToIterate)
                        {
                            bTree.Delete("B" + value);
                            bTreeValues.Remove(value);
                        }                                                   
                    }
                    else if (generator.Next(2) == 0)
                    {
                        // Add on A
                        for (int i = 0; i < 100; i++)
                        {
                            aTree.Add("A" + generator.Next(), new byte[0]);
                        }                        
                        
                    }
                    else
                    {
                        // Add on B                        
                        for (int i = 0; i < 100; i++)
                        {
                            int bKey = generator.Next();
                            if (!bTreeValues.ContainsKey(bKey))
                            {
                                bTree.Add("B" + bKey, new byte[0]);
                                bTreeValues[bKey] = bKey;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var aTree = tx.Environment.CreateTree(tx, "ATree");

                var startWithValue = new Slice("A");

                var reader = new SnapshotReader(tx);
                using (var iterator = reader.Iterate("ATree"))
                {
                    iterator.Seek(Slice.BeforeAllKeys);
                    do
                    {
                        if (iterator.CurrentKey != Slice.AfterAllKeys && iterator.CurrentKey != Slice.BeforeAllKeys)
                        {
                            Assert.True(iterator.CurrentKey.StartsWith(startWithValue));
                        }
                    }
                    while (iterator.MoveNext());
                }
            }

        }
    }
}
