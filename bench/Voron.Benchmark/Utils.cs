using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Server;
using Voron.Data.Tables;

namespace Voron.Benchmark
{
    public class Utils
    {
        public static List<Tuple<Slice, Slice>> GenerateUniqueRandomSlicePairs(int amount, int keyLength, int? randomSeed = null)
        {
            Debug.Assert(amount > 0);
            Debug.Assert(keyLength > 0);

            // Generate random key value pairs
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            var keyBuffer = new byte[keyLength];

            // This serves to ensure the uniqueness of keys globally (that way
            // we know the exact number of insertions)
            var added = new HashSet<Slice>(SliceComparer.Instance);
            var pairs = new List<Tuple<Slice, Slice>>();
            int i = 0;

            while (pairs.Count < amount)
            {
                Slice key;
                Slice value;

                generator.NextBytes(keyBuffer);

                ByteStringContext.Scope keyScope =
                    Slice.From(Configuration.Allocator, keyBuffer, ByteStringType.Immutable, out key);

                i++;

                if (added.Contains(key))
                {
                    // Release the unused key's memory
                    keyScope.Dispose();
                    continue;
                }

                // Trees are mostly used by Table to store long values. We
                // attempt to emulate that behavior
                long valueBuffer = generator.Next();
                valueBuffer += (long)generator.Next() << 32;
                valueBuffer += (long)generator.Next() << 64;
                valueBuffer += (long)generator.Next() << 96;

                unsafe
                {
                    Slice.From(Configuration.Allocator, (byte*)&valueBuffer, sizeof(long), ByteStringType.Immutable, out value);
                }

                pairs.Add(new Tuple<Slice, Slice>(key, value));
                added.Add(key);
            }

            return pairs;
        }

        public static List<Slice> GenerateWornoutTree(
            StorageEnvironment env,
            Slice treeNameSlice,
            int generationTreeSize,
            int generationBatchSize,
            int keyLength,
            double generationDeletionProbability,
            int? randomSeed
            )
        {
            List<Slice> treeKeys = new List<Slice>();
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value): new Random();
            bool hasTree = false;

            using (var tx = env.ReadTransaction())
            {
                hasTree = tx.ReadTree(treeNameSlice) != null;
                tx.Commit();
            }

            if (hasTree)
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree(treeNameSlice);

                    using (var it = tree.Iterate(false))
                    {
                        if (it.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                treeKeys.Add(it.CurrentKey.Clone(Configuration.Allocator, ByteStringType.Immutable));
                            } while (it.MoveNext());
                        }
                    }

                    tx.Commit();
                }
            }
            else
            {
                // Create a tree with enough wearing
                using (var tx = env.WriteTransaction())
                {
                    var values = new List<Tuple<Slice, Slice>>();
                    var tree = tx.CreateTree(treeNameSlice);

                    while (tree.State.NumberOfEntries < generationTreeSize)
                    {
                        int deletions = 0;

                        // Add BatchSize new keys
                        for (int i = 0; i < generationBatchSize; i++)
                        {
                            // We might run out of values while creating the tree, generate more.
                            if (values.Count == 0)
                            {
                                values = GenerateUniqueRandomSlicePairs(
                                    generationTreeSize,
                                    keyLength,
                                    randomSeed);
                            }

                            var pair = values[0];
                            values.RemoveAt(0);

                            // Add it to the tree key set
                            treeKeys.Add(pair.Item1);

                            // Add it to the tree
                            tree.Add(pair.Item1, pair.Item2);

                            // Simulate a binomial rv in the mean time
                            if (generator.NextDouble() < generationDeletionProbability)
                            {
                                deletions++;
                            }
                        }

                        // Delete the number of deletions given by the binomial rv
                        // We may have gone a little bit over the limit during
                        // insertion, but we rebalance here.
                        if (tree.State.NumberOfEntries > generationTreeSize)
                        {
                            while (tree.State.NumberOfEntries > generationTreeSize)
                            {
                                var keyIndex = generator.Next(treeKeys.Count);
                                tree.Delete(treeKeys[keyIndex]);
                                treeKeys.RemoveAt(keyIndex);
                            }
                        }
                        else
                        {
                            while (deletions > 0 && tree.State.NumberOfEntries > 0)
                            {
                                var keyIndex = generator.Next(treeKeys.Count);
                                tree.Delete(treeKeys[keyIndex]);
                                treeKeys.RemoveAt(keyIndex);
                                deletions--;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            return treeKeys;
        }

        public static List<Slice> GenerateWornoutTable(
            StorageEnvironment env,
            Slice tableNameSlice,
            TableSchema schema,
            int generationTableSize,
            int generationBatchSize,
            int keyLength,
            double generationDeletionProbability,
            int? randomSeed
            )
        {
            var tableKeys = new List<Slice>();
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            bool hasTable;

            using (var tx = env.ReadTransaction())
            {
                try
                {
                    tx.OpenTable(schema, tableNameSlice);
                    hasTable = true;
                }
                catch (Exception)
                {
                    hasTable = false;
                }

                tx.Commit();
            }

            if (hasTable)
            {
                using (var tx = env.ReadTransaction())
                {
                    var table = tx.OpenTable(schema, tableNameSlice);

                    foreach (var reader in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                    {
                        Slice key;
                        schema.Key.GetSlice(Configuration.Allocator, ref reader.Reader, out key);
                        tableKeys.Add(key);
                    }

                    tx.Commit();
                }
            }
            else
            {
                // Create a table with enough wearing
                using (var tx = env.WriteTransaction())
                {
                    var values = new List<Tuple<Slice, Slice>>();
                    schema.Create(tx, tableNameSlice, 16);
                    var table = tx.OpenTable(schema, tableNameSlice);

                    while (table.NumberOfEntries < generationTableSize)
                    {
                        int deletions = 0;

                        // Add BatchSize new keys
                        for (int i = 0; i < generationBatchSize; i++)
                        {
                            // We might run out of values while creating the table, generate more.
                            if (values.Count == 0)
                            {
                                values = GenerateUniqueRandomSlicePairs(
                                    generationTableSize,
                                    keyLength,
                                    randomSeed);
                            }

                            var pair = values[0];
                            values.RemoveAt(0);

                            // Add it to the table key set
                            tableKeys.Add(pair.Item1);

                            // Add it to the table
                            table.Insert(new TableValueBuilder
                                {
                                    pair.Item1,
                                    pair.Item2
                                });

                            // Simulate a binomial rv in the mean time
                            if (generator.NextDouble() < generationDeletionProbability)
                            {
                                deletions++;
                            }
                        }

                        // Delete the number of deletions given by the binomial rv
                        // We may have gone a little bit over the limit during
                        // insertion, but we rebalance here.
                        if (table.NumberOfEntries > generationTableSize)
                        {
                            while (table.NumberOfEntries > generationTableSize)
                            {
                                var keyIndex = generator.Next(tableKeys.Count);
                                table.DeleteByKey(tableKeys[keyIndex]);
                                tableKeys.RemoveAt(keyIndex);
                            }
                        }
                        else
                        {
                            while (deletions > 0 && table.NumberOfEntries > 0)
                            {
                                var keyIndex = generator.Next(tableKeys.Count);
                                table.DeleteByKey(tableKeys[keyIndex]);
                                tableKeys.RemoveAt(keyIndex);
                                deletions--;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            return tableKeys;
        }
    }
}
