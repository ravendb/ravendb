using FastTests.Voron.Compact;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Voron
{
    public class PrefixTreeTable : PrefixTreeStorageTests
    {
        [Fact]
        public void Structure_RandomTester()
        {
            int count = 100;
            int size = 5;
            for (int i = 0; i < 1; i++)
            {
                InitializeStorage(Name + i);

                var keys = new Slice[count];

                var insertedKeys = new HashSet<string>();

                using (var tx = Env.WriteTransaction())
                {
                    var docs = new Table(DocsSchema, Name + i, tx);
                    var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                    var generator = new Random(i + size);
                    for (int j = 0; j < count; j++)
                    {
                        string key = GenerateRandomString(generator, size);
                        var keySlice = new Slice(Encoding.UTF8.GetBytes(key));

                        if (!insertedKeys.Contains(key))
                        {
                            Assert.False(tree.Contains(keySlice));
                            Assert.NotEqual(-1, AddToPrefixTree(tree, docs, keySlice, key));
                        }

                        keys[j] = keySlice;
                        insertedKeys.Add(key);

                        StructuralVerify(tree);
                    }

                    tx.Commit();
                }

                using (var tx = Env.ReadTransaction())
                {
                    var docs = new Table(DocsSchema, Name + i, tx);
                    var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                    StructuralVerify(tree);

                    Assert.Equal(insertedKeys.Count, tree.Count);
                }

                using (var tx = Env.WriteTransaction())
                {
                    var docs = new Table(DocsSchema, Name + i, tx);
                    var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                    // The chance that we hit an already existing key is very low with a different seed so checking the inserted key is probably going to return false.
                    var generator = new Random(i + size + 1);
                    for (int j = 0; j < count; j++)
                    {
                        string key = GenerateRandomString(generator, size);
                        var keySlice = new Slice(Encoding.UTF8.GetBytes(key));

                        if (!insertedKeys.Contains(key))
                            Assert.False(tree.Delete(keySlice));
                    }

                    StructuralVerify(tree);

                    // We reply the insertion order to delete. 
                    generator = new Random(i + size);
                    for (int j = 0; j < count; j++)
                    {
                        string key = GenerateRandomString(generator, size);
                        var keySlice = new Slice(Encoding.UTF8.GetBytes(key));

                        bool removed = tree.Delete(keySlice);
                        Assert.True(removed);

                        if (j % 10 == 0)
                            StructuralVerify(tree);
                    }

                    StructuralVerify(tree);

                    tx.Commit();
                }

                using (var tx = Env.ReadTransaction())
                {
                    var docs = new Table(DocsSchema, Name + i, tx);
                    var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                    StructuralVerify(tree);

                    Assert.Equal(0, tree.Count);
                }

            }
        }

        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size)
        {
            var stringChars = new char[size];
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new string(stringChars);
        }

        public void Structure_GenerateTestCases_MultipleTx()
        {
            int entries = 100;
            int iterations = 100;
            int size = 5;
            int txSize = 1;

            string[] smallestRepro = null;
            int smallest = int.MaxValue;

            try
            {
                for (int i = 0; i < iterations; i++)
                {
                    var keys = new string[entries];
                    var transactionEnd = new bool[entries];

                    var insertedKeys = new HashSet<string>();
                    var generator = new Random(i + size);

                    try
                    {
                        int counter = 0;
                        for (int transactions = 0; transactions < entries / txSize; transactions++)
                        {
                            Env.FlushLogToDataFile();
                            using (var tx = Env.WriteTransaction())
                            {
                                var docs = new Table(DocsSchema, Name + i, tx);
                                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                                for (int j = 0; j < txSize; j++)
                                {
                                    string key = GenerateRandomString(generator, size);
                                    var keySlice = new Slice(Encoding.UTF8.GetBytes(key));

                                    if (!insertedKeys.Contains(key))
                                    {
                                        keys[counter] = key;
                                        insertedKeys.Add(key);

                                        Assert.False(tree.Contains(keySlice));
                                        Assert.NotEqual(-1, AddToPrefixTree(tree, docs, keySlice, "8Jp3"));

                                        counter++;
                                    }

                                    StructuralVerify(tree);
                                }

                                // transactionEnd[transactions * (txSize + 1) - 1] = true;
                                tx.Commit();
                            }
                        }
                    }
                    catch
                    {
                        Console.WriteLine($"Found one of size {insertedKeys.Count}.");
                        if (smallest > insertedKeys.Count)
                        {
                            smallest = insertedKeys.Count;
                            smallestRepro = keys;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Something else happens.");
                Console.WriteLine(e);
                Console.WriteLine("Best found before failing:");
            }

            if (smallest != int.MaxValue)
            {
                Console.WriteLine();
                Console.WriteLine($"Found best error of size: {smallest}");
                for (int i = 0; i < smallest; i++)
                    Console.WriteLine(smallestRepro[i]);
            }
            else Console.WriteLine("No structural fail found.");

            Console.WriteLine("Done!");
            Console.ReadLine();
        }

        public void Structure_GenerateTestCases()
        {
            int entries = 500;
            int iterations = 1;
            int size = 5;

            string[] smallestRepro = null;
            int smallest = int.MaxValue;

            try
            {
                for (int i = 0; i < iterations; i++)
                {
                    var keys = new string[entries];

                    var insertedKeys = new HashSet<string>();

                    using (var tx = Env.WriteTransaction())
                    {
                        var docs = new Table(DocsSchema, Name + i, tx);
                        var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                        var generator = new Random(i + size);

                        try
                        {
                            for (int j = 0; j < entries; j++)
                            {
                                string key = GenerateRandomString(generator, size);
                                var keySlice = new Slice(Encoding.UTF8.GetBytes(key));

                                if (!insertedKeys.Contains(key))
                                {
                                    keys[j] = key;
                                    insertedKeys.Add(key);

                                    Assert.False(tree.Contains(keySlice));
                                    Assert.NotEqual(-1, AddToPrefixTree(tree, docs, keySlice, "8Jp3"));
                                }

                                StructuralVerify(tree);
                            }
                        }
                        catch
                        {
                            Console.WriteLine($"Found one of size {insertedKeys.Count}.");
                            if (smallest > insertedKeys.Count)
                            {
                                smallest = insertedKeys.Count;
                                smallestRepro = keys;
                            }
                        }

                        // tx.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Something else happens.");
                Console.WriteLine(e);
                Console.WriteLine("Best found before failing:");
            }

            if (smallest != int.MaxValue)
            {
                Console.WriteLine();
                Console.WriteLine($"Found best error of size: {smallest}");
                for (int i = 0; i < smallest; i++)
                    Console.WriteLine(smallestRepro[i]);
            }
            else Console.WriteLine("No structural fail found.");

            Console.WriteLine("Done!");
            Console.ReadLine();
        }
    }
}
