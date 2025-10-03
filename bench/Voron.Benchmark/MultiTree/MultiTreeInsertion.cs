using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron.Benchmark.MultiTree;

public class MultiTreeInsertion : StorageBenchmark
{
    public override bool DeleteBeforeEachBenchmark { get; protected set; } = false;


    private int _seed = 21341;

    [Params(100, 1000, 10_000)]
    public int KeyCount { get; set; }

    [Params(1, 100, 1000)]
    public int ValuesCount { get; set; }

    private ByteStringContext _allocator;

    private List<Slice> _keys;
    private List<List<Slice>> _values;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var random = new Random(_seed);
        var dataToIndex = new Dictionary<Slice, List<Slice>>(SliceComparer.Instance);

        for (var keyIdx = 0; keyIdx < KeyCount; keyIdx++)
        {
            var key = RandomString(random, 10);
            var scope = Slice.From(_allocator, key, out var keyAsSlice);
            ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(dataToIndex, keyAsSlice, out var exists);
            if (exists)
            {
                scope.Dispose();
                keyIdx--;
                continue;
            }


            list ??= new();
            var tempHashSet = new HashSet<string>();
            for (var valueIdx = 0; valueIdx < ValuesCount && tempHashSet.Count < ValuesCount; valueIdx++)
            {
                var value = RandomString(random, 16);
                tempHashSet.Add(value);
            }

            foreach (var temp in tempHashSet)
            {
                Slice.From(_allocator, temp, out var valueAsSlice);
                list.Add(valueAsSlice);
            }

            CollectionsMarshal.AsSpan(list).Sort(SliceComparer.Instance);
        }

        _keys = new List<Slice>(dataToIndex.Count);
        _values = new List<List<Slice>>(dataToIndex.Count);
        foreach (var (key, values) in dataToIndex)
        {
            _keys.Add(key);
            _values.Add(values);
        }

        CollectionsMarshal.AsSpan(_keys).Sort(CollectionsMarshal.AsSpan(_values), SliceComparer.Instance);
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


    [Benchmark(Baseline = true)]
    public void MultiAdd()
    {
        using var wTx = Env.WriteTransaction();
        var mT = wTx.CreateTree(nameof(MultiAdd));
        for (int i = 0; i < _keys.Count; i++)
        {
            var key = _keys[i];
            foreach (var value in _values[i])
            {
                mT.MultiAdd(key, value);
            }
        }
    }

    [Benchmark]
    public void MultiBulkAdd()
    {
        using var wTx = Env.WriteTransaction();
        var mT = wTx.CreateTree(nameof(MultiAdd));
        for (int i = 0; i < _keys.Count; i++)
        {
            var key = _keys[i];
            var values = _values[i];
            mT.MultiBulkAdd(key, CollectionsMarshal.AsSpan(values));
        }
    }
}
