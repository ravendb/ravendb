using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25556(ITestOutputHelper output) : StorageTest(output)
{
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(45, 733276104)]
    [InlineData(15, 1886410083)]
    [InlineData(50, 216822873)]
    [InlineDataWithRandomSeed(50)]
    public void ExtendingNestedPageInsideMultiValueTreeHasCorrectOffsetToTheActualUpdatedData(int iterationsToRun, int seed)
    {
        const int maxIterations = 1_000;
        const int bulkSize = 4096;
        var random = new Random(seed);
        
        var collectionNames = Enumerable.Range(0, 11)
            .Select(_ => RandomString(random, random.Next(10, 18)))
            .ToList();
        var mainCollection = collectionNames[random.Next(collectionNames.Count)];
        collectionNames.Remove(mainCollection);
        
        var currentId = 0;
        var idsOrder = Enumerable.Range(random.Next(0, 20_000_000), maxIterations * bulkSize).ToArray();
        random.Shuffle(idsOrder); // random insert

        for (int transactionIdx = 0; transactionIdx < iterationsToRun; ++transactionIdx)
        {
            using var wTx = Env.WriteTransaction();
            var mt = wTx.CreateTree("Test");
            Dictionary<Slice, List<Slice>> bulkInsertContainer = new (SliceComparer.Instance);
            for (int keyIdx = 0; keyIdx < bulkSize; ++keyIdx)
            {
                var collectionsAsSpan = CollectionsMarshal.AsSpan(collectionNames);
                random.Shuffle(collectionsAsSpan);
                var valuesToInsert = random.Next(3, collectionNames.Count);
                var valuesForKey = Enumerable.Range(0, valuesToInsert)
                    .Select(i => $"{collectionNames[i]}/{random.Next()}").ToArray();
                
                var currentKey = $"{mainCollection}/{idsOrder[currentId++]}";
                List<Slice> valuesAsSlice = new();
                
                foreach (var valueToInsert in valuesForKey)
                {
                    // We're leaking the memory here. It will be released via transaction release.
                    Slice.From(wTx.Allocator, valueToInsert, out var slice);
                    valuesAsSlice.Add(slice);
                }
                
                // We're randomly inserting keys. We're inserting only one value here
                // since MultiBulkAdd has optimization for a new insertion.
                mt.MultiAdd(currentKey, valuesAsSlice[^1]);
                valuesAsSlice.RemoveAt(valuesAsSlice.Count - 1);
                valuesAsSlice.Sort(SliceComparer.Instance);
                
                Slice.From(wTx.Allocator, currentKey, out var currentKeySlice);
                bulkInsertContainer.Add(currentKeySlice, valuesAsSlice);
            }
            
            // Insert the rest of the values in bulks.
            foreach (var (key, values) in bulkInsertContainer)
            {
                mt.MultiBulkAdd(key, CollectionsMarshal.AsSpan(values));
            }
            
            wTx.Commit();
        }
    }
    
    private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static string RandomString(Random random, int size)
    {
        var buffer = new char[size];

        for (int i = 0; i < size; i++)
        {
            buffer[i] = Chars[random.Next(Chars.Length)];
        }

        return new string(buffer);
    }
}
