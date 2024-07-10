using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron.FixedSize;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenDB_22561_LLT : NoDisposalNeeded
{
    public RavenDB_22561_LLT(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed(100)]
    [InlineDataWithRandomSeed(1_000)]
    [InlineDataWithRandomSeed(10_000)]
    public void CanHeapSortDataLong(int size, int seed)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var random = new System.Random(seed);
        var table = Enumerable.Range(0, size).Select(x => (long)random.Next(-1_000_000, 1_000_000)).ToArray();
        var randomToDuplicate = random.Next(0, size - 10);
        for (int i = 1; i < randomToDuplicate + 10; ++i)
            table[i] = table[randomToDuplicate];

        random.Shuffle(table);

        for (int pageSize = (size / 10); pageSize <= size; pageSize += (size / 10))
        {
            var indexes = new int[pageSize].AsSpan();
            var terms = new long[pageSize].AsSpan();
            var sorter = global::Corax.Querying.Matches.SortingMatches.HeapSorterBuilder.BuildSingleNumericalSorter(indexes, terms, false);

            for (var docPos = 0; docPos < size; ++docPos)
                sorter.Insert(docPos, table[docPos]);

            ContextBoundNativeList<long> result = new ContextBoundNativeList<long>(bsc);
            sorter.Fill(table, ref result);
            Assert.Equal(table.OrderBy(x => x).Take(pageSize).ToArray().AsSpan(), result.ToSpan());
            result.Dispose();
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed(100)]
    [InlineDataWithRandomSeed(1_000)]
    [InlineDataWithRandomSeed(10_000)]
    public void CanHeapSortDataLongCompound(int size, int seed)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var random = new System.Random(seed);
        var table = Enumerable.Range(0, size).Select(x => (long)random.Next(size / 10)).ToArray();
        var secondTable = Enumerable.Range(0, size).Select(x => (long)random.Next(-1_000_000, 1_000_000)).ToArray();
        var randomToDuplicate = random.Next(0, size - 10);
        for (int i = 1; i < randomToDuplicate + 10; ++i)
            table[i] = table[randomToDuplicate];

        random.Shuffle(table);

        Assert.NotEqual(table.ToHashSet().Count, size);

        for (int pageSize = (size / 10); pageSize <= size; pageSize += (size / 10))
        {
            var indexes = new int[pageSize].AsSpan();
            var terms = new long[pageSize].AsSpan();
            var sorter = global::Corax.Querying.Matches.SortingMatches.HeapSorterBuilder.BuildCompoundNumericalSorter(indexes, terms, false,
                new TestLongComparable(secondTable));

            for (var docPos = 0; docPos < size; ++docPos)
                sorter.Insert(docPos, table[docPos]);

            ContextBoundNativeList<long> result = new ContextBoundNativeList<long>(bsc);
            sorter.Fill(table, ref result);
            Assert.Equal(table.Zip(secondTable).OrderBy(x => x.First).ThenBy(x => x.Second).Take(pageSize).Select(x => x.First).ToArray().AsSpan(), result.ToSpan());
            result.Dispose();
        }
    }

    private class TestLongComparable(long[] table) : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            return table[x].CompareTo(table[y]);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed(100)]
    [InlineDataWithRandomSeed(1_000)]
    [InlineDataWithRandomSeed(10_000)]
    public void CanHeapSortDataString(int size, int seed)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var random = new System.Random(seed);
        var sourceData = Enumerable.Range(0, size).Select(x => RandomString(random.Next(6, 16))).ToArray();
        var randomToDuplicate = random.Next(0, size - 10);
        for (int i = 1; i < randomToDuplicate + 10; ++i)
            sourceData[i] = sourceData[randomToDuplicate];

        var positions = Enumerable.Range(0, size).Select(x => (long)x).ToArray();

        for (int pageSize = (size / 10); pageSize <= size; pageSize += (size / 10))
        {
            var indexes = new int[pageSize].AsSpan();
            var terms = new ByteString[pageSize].AsSpan();
            var sorter = global::Corax.Querying.Matches.SortingMatches.HeapSorterBuilder.BuildSingleAlphanumericalSorter(indexes, terms, bsc, false);

            for (var docPos = 0; docPos < size; ++docPos)
                sorter.Insert(docPos, Encodings.Utf8.GetBytes(sourceData[docPos]));

            ContextBoundNativeList<long> result = new ContextBoundNativeList<long>(bsc);
            sorter.Fill(positions, ref result);

            var expectedOrder = sourceData.OrderBy(x => x, AlphaNumericFieldComparator.StringAlphanumComparer.Instance).Take(pageSize).ToArray();
            List<string> orderFromSorter = new();
            foreach (var pos in result)
                orderFromSorter.Add(sourceData[pos]);

            Assert.Equal(expectedOrder.AsSpan(), orderFromSorter.ToArray().AsSpan());
            result.Dispose();
        }


        random.Shuffle(sourceData);

        string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            var str = new char[length];
            for (int i = 0; i < length; i++)
            {
                str[i] = chars[random.Next(chars.Length)];
            }

            return new string(str);
        }
    }
}

public class RavenDB_22561 : RavenTestBase
{
    public RavenDB_22561(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CompoundAlphanumeric()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        var list = new Dto[20];
        for (int i = 0; i < 10; ++i)
        {
            var doc2 = new Dto($"Maciej/{2 * i}", 2 * i + 1);
            var doc1 = new Dto($"Maciej/{2 * i}", 2 * i);
            list[2 * i] = doc1;
            list[2 * i + 1] = doc2;
        }

        var rand = new Random(123154);
        rand.Shuffle(list);

        // lets store them in random order
        foreach (var dto in list)
            session.Store(dto);

        session.SaveChanges();

        List<Dto> dtos = session.Query<Dto>()
            .Customize(x => x.WaitForNonStaleResults())
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .ThenByDescending(x => x.Number, OrderingType.Long)
            .ToList();

        Assert.Equal(20, dtos.Count);
        for (int i = 0; i < 10; ++i)
        {
            Assert.Equal(dtos[i * 2].Name, $"Maciej/{2 * i}");
            Assert.Equal(dtos[i * 2 + 1].Name, $"Maciej/{2 * i}");
            Assert.Equal(dtos[i * 2].Number, 2 * i + 1);
            Assert.Equal(dtos[i * 2 + 1].Number, 2 * i);
        }

        dtos = session.Query<Dto>()
            .Customize(x => x.WaitForNonStaleResults())
            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
            .ThenBy(x => x.Number, OrderingType.Long)
            .ToList();

        Assert.Equal(20, dtos.Count);
        for (int i = 0; i < 10; ++i)
        {
            Assert.Equal(dtos[i * 2].Name, $"Maciej/{2 * i}");
            Assert.Equal(dtos[i * 2 + 1].Name, $"Maciej/{2 * i}");
            Assert.Equal(dtos[i * 2].Number, 2 * i);
            Assert.Equal(dtos[i * 2 + 1].Number, 2 * i + 1);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CompoundAlphanumericAsSecond()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        var list = new Dto[20];
        for (int i = 0; i < 10; ++i)
        {
            var doc2 = new Dto($"Maciej/{2 * i + 1}", 2 * i);
            var doc1 = new Dto($"Maciej/{2 * i}", 2 * i);
            list[2 * i] = doc1;
            list[2 * i + 1] = doc2;
        }

        var rand = new Random(123154);
        rand.Shuffle(list);

        // lets store them in random order
        foreach (var dto in list)
            session.Store(dto);

        session.SaveChanges();

        List<Dto> dtos = session.Query<Dto>()
            .Customize(x => x.WaitForNonStaleResults())
            .OrderBy(x => x.Number, OrderingType.Long)
            .ThenByDescending(x => x.Name, OrderingType.AlphaNumeric)
            .ToList();

        Assert.Equal(20, dtos.Count);
        for (int i = 0; i < 10; ++i)
        {
            Assert.Equal(dtos[i * 2].Number, 2 * i);
            Assert.Equal(dtos[i * 2 + 1].Number, 2 * i);
            Assert.Equal(dtos[i * 2].Name, $"Maciej/{2 * i + 1}");
            Assert.Equal(dtos[i * 2 + 1].Name, $"Maciej/{2 * i}");
        }

        dtos = session.Query<Dto>()
            .Customize(x => x.WaitForNonStaleResults())
            .OrderBy(x => x.Number, OrderingType.Long)
            .ThenBy(x => x.Name, OrderingType.AlphaNumeric)
            .ToList();

        Assert.Equal(20, dtos.Count);
        for (int i = 0; i < 10; ++i)
        {
            Assert.Equal(dtos[i * 2].Number, 2 * i);
            Assert.Equal(dtos[i * 2 + 1].Number, 2 * i);
            Assert.Equal(dtos[i * 2].Name, $"Maciej/{2 * i}");
            Assert.Equal(dtos[i * 2 + 1].Name, $"Maciej/{2 * i + 1}");
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NullAlphanumerical()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        session.Store(new Dto("Maciej", 0));
        session.Store(new Dto(null, 1));
        session.SaveChanges();

        Dto[] results = session.Query<Dto>().OrderBy(x => x.Name, OrderingType.AlphaNumeric).ToArray();
        Assert.Equal(2, results.Length);
    }

    private record Dto(string Name, long Number, string Id = null);
}
