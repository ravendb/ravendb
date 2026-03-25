using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25033(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanUpdateAlreadyNestedPageWithDuplicatesInInsertion()
    {
        //This is low-level test based on RDBC_128;
        var data = ReadFileToList("SlowTests.Data.RavenDB_25033.RavenDB_25033_RDBC_128.txt");
        MultiTreeTransactionsReplayer(data,
            keyPrefix: "sy",
            keySuffix: string.Empty,
            valuePrefix: "invoices/",
            valueSuffix: "-a");
    }

    private List<string> ReadFileToList(string path)
    {
        var assembly = typeof(RavenDB_25033).Assembly;
        using (var fs = assembly.GetManifestResourceStream(path))
        using (var reader = new StreamReader(fs))
        {
            string line;

            var results = new List<string>();

            while (string.IsNullOrEmpty(line = reader.ReadLine()) == false)
            {
                results.Add(line.Trim());
            }

            return results;
        }
    }

    /// <summary>
    /// Allows replaying operations on MultiTree.
    /// Schema:
    /// [OPERATION][KEY](|[VALUE])+
    /// Examples:
    /// +0|1 adds value 1 to key 0
    /// ++0|1|2|3|4 adds values 1,2,3,4 to key 0
    /// -0|1 removes value 1 from key 0
    /// # commits transaction
    ///
    /// Also prefixes and suffixes can be specified.
    /// It is useful to decrease the size of the file since often those are unified.
    /// </summary>
    private void MultiTreeTransactionsReplayer(List<string> source, string keyPrefix, string keySuffix, string valuePrefix, string valueSuffix)
    {
        source.Add("CHECK");
        Transaction tx = null;
        Tree tree = null;
        Dictionary<string, HashSet<string>> multiTreeInMemory = new();

        try
        {
            tx = Env.WriteTransaction();
            tree = tx.CreateTree("test");
            var commited = false;
            int commitCounter = 0;

            for (int commandIdx = 0; commandIdx < source.Count; commandIdx++)
            {
                string command = source[commandIdx];
                if (commited || command == "CHECK")
                {
                    AssertTree();
                    if (command == "CHECK")
                        continue;
                }

                if (command.StartsWith('#'))
                {
                    tx.Commit();
                    tx.Dispose();
                    commited = true;
                    tx = Env.WriteTransaction();
                    tree = tx.CreateTree("test");
                    commitCounter++;
                    continue;
                }

                commited = false;
                var firstIndexOf = command.IndexOf('|');
                string key;
                if (command.StartsWith("++"))
                {
                    key = $"{keyPrefix}{command[2..firstIndexOf]}{keySuffix}";
                    var valuesToIndex = command[(firstIndexOf + 1)..].Split('|').Select(value => $"{valuePrefix}{value}{valueSuffix}");
                    List<IDisposable> disposables = new();
                    List<Slice> slices = new();

                    ref var inMemoryValues = ref CollectionsMarshal.GetValueRefOrAddDefault(multiTreeInMemory, key, out var _);
                    inMemoryValues ??= new();
                    
                    foreach (var val in valuesToIndex)
                    {
                        inMemoryValues.Add(val);
                        disposables.Add(Slice.From(tx.Allocator, val, out var slice));
                        slices.Add(slice);
                    }


                    using var _ = Slice.From(tx.Allocator, key, out var keyAsSlice);
                    tree.MultiBulkAdd(keyAsSlice, CollectionsMarshal.AsSpan(slices));

                    foreach (var d in disposables)
                        d.Dispose();

                    AssertTree();
                }

                key = $"{keyPrefix}{command[1..firstIndexOf]}{keySuffix}";
                var value = $"{valuePrefix}{command[(firstIndexOf + 1)..]}{valueSuffix}";
                if (command.StartsWith("-"))
                {
                    tree.MultiDelete(key, value);
                    multiTreeInMemory[key].Remove(value);
                    if (multiTreeInMemory[key].Count == 0)
                        multiTreeInMemory.Remove(key);

                    AssertTree();
                }

                if (command.StartsWith("+"))
                {
                    tree.MultiAdd(key, value);
                    ref var valuesList = ref CollectionsMarshal.GetValueRefOrAddDefault(multiTreeInMemory, key, out var _);
                    valuesList ??= new();
                    valuesList.Add(value);

                    AssertTree();
                }
            }

            Assert.True(commited);
        }
        finally
        {
            tx?.Dispose();
        }

        void AssertTree()
        {
            // We're going to assert that trees are equal
            using var keyIt = tree.Iterate(false);
            var hasKeys = keyIt.Seek(Slices.BeforeAllKeys);
            Assert.Equal(multiTreeInMemory.Count > 0, hasKeys);
            List<string> checkedKeys = new();
            do
            {
                checkedKeys.Add(keyIt.CurrentKey.ToString());
                List<string> valuesFromTree = new();
                using var valuesIt = tree.MultiRead(keyIt.CurrentKey);
                Assert.True(valuesIt.Seek(Slices.BeforeAllKeys)); // it must have at least one value
                do
                {
                    valuesFromTree.Add(valuesIt.CurrentKey.ToString());
                } while (valuesIt.MoveNext());

                Assert.Equal(multiTreeInMemory[keyIt.CurrentKey.ToString()].Count, valuesFromTree.Count);
                Assert.NotEmpty(multiTreeInMemory[keyIt.CurrentKey.ToString()]);
                Assert.NotEmpty(valuesFromTree);
                Assert.Empty(valuesFromTree.Except(multiTreeInMemory[keyIt.CurrentKey.ToString()]));
                Assert.Empty(multiTreeInMemory[keyIt.CurrentKey.ToString()].Except(valuesFromTree));
            } while (keyIt.MoveNext());

            Assert.Equal(multiTreeInMemory.Count, checkedKeys.Count);
            Assert.Empty(checkedKeys.Except(multiTreeInMemory.Keys));
            Assert.Empty(multiTreeInMemory.Keys.Except(checkedKeys));
        }
    }
}
