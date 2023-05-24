using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Utils;

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(TypeCacheConfig))]
    public class TypeCacheBenchmark
    {
        private class TypeCacheConfig : ManualConfig
        {
            public TypeCacheConfig()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core70,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit,
                    },
                    Run =
                    {
                        // TODO: Next line is just for testing. Fine tune parameters.
                        //RunStrategy = RunStrategy.Monitoring,
                    }
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                AddExporter(RPlotExporter.Default);

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private Type[] _typesToCheck;

        private readonly RavenDb50TypeCache<int> _ravenDb50TypeCache = new(128);
        private readonly UnsafeAddressTypeCache<int> _compareExchangeThreadSafe = new(128);
        private readonly HashFastTypeCache<int> _hashCompareExchangeThreadSafe = new(128);
        private readonly Dictionary<Type, int> _dictionary = new(128);


        [GlobalSetup]
        public void Setup()
        {
            var typesToCheck = new[]
            {
                typeof(Random), typeof(FastList<int>), typeof(GlobalCleanupAttribute), typeof(TypedReference), typeof(Array), typeof(AccessViolationException),
                typeof(ReadOnlySpan<int>), typeof(GC), typeof(EnumerableExecutor), typeof(LdapStyleUriParser), typeof(ConsoleKey), typeof(Enumerable)
            };

            var rnd = new Random();
            _typesToCheck = new Type[1000];
            for (int i = 0; i < _typesToCheck.Length; i++)
            {
                _typesToCheck[i] = typesToCheck[ rnd.Next(typesToCheck.Length) ];
            }

            foreach (var type in typesToCheck)
            {
                _ravenDb50TypeCache.Put(type, 0);
                _compareExchangeThreadSafe.Put(type, 0);
                _hashCompareExchangeThreadSafe.Set(type, 0);
                _dictionary.Add(type, 0);
            }
        }


        [Benchmark(Baseline = true)]
        public int CurrentThreadUnsafe()
        {
            int result = 0;
            foreach (var type in _typesToCheck)
            {
                _ravenDb50TypeCache.TryGet(type, out var r);
                result += r;
            }

            return result;
        }

        [Benchmark]
        public int CompareExchangeThreadSafe()
        {
            int result = 0;
            foreach (var type in _typesToCheck)
            {
                _compareExchangeThreadSafe.TryGet(type, out var r);
                result += r;
            }

            return result;
        }

        [Benchmark]
        public int HashCompareExchangeThreadSafe()
        {
            int result = 0;
            foreach (var type in _typesToCheck)
            {
                _hashCompareExchangeThreadSafe.TryGet(type, out var r);
                result += r;
            }

            return result;
        }

        [Benchmark]
        public int DictionaryThreadUnsafe()
        {
            int result = 0;
            foreach (var type in _typesToCheck)
            {
                _dictionary.TryGetValue(type, out var r);
                result += r;
            }

            return result;
        }

        public class UnsafeAddressTypeCache<T> 
        {
            private struct CacheItem
            {
                public Type Type;
                public T Value;
            }

            private readonly int _andMask;
            private CacheItem[] _cache;

            public UnsafeAddressTypeCache(int cacheSize = 8)
            {
                if (!Bits.IsPowerOfTwo(cacheSize))
                    cacheSize = Bits.PowerOf2(cacheSize);

                int shiftRight = Bits.CeilLog2(cacheSize);
                _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

                _cache = ArrayPool<CacheItem>.Shared.Rent(cacheSize);
            }

            public bool TryGet(Type t, out T value)
            {
                unsafe
                {
                    // PERF: This only works because we assume that Type will not change the memory location over time.
                    // If that would not be the case we would be causing lots of cache misses, which is not the intended
                    // use of this facility. The idea is to be incredibly fast to retrieve a value if it exist in the cache
                    // even at the expense of failing to recognize that it is there.
                    TypedReference reference = __makeref(t);
                    ulong extremelyUnsafeAddress = **(ulong**)&reference;

                    var position = (int)extremelyUnsafeAddress & _andMask;

                    // We are effectively synchronizing the access to the cache because we cannot copy the whole 
                    // node to the stack in a single atomic operation. 
                    CacheItem[] cache;
                    do
                    {
                        // Because we are assigning null, noone else can continue from here. 
                        cache = Interlocked.Exchange(ref _cache, null);
                    }
                    while (cache == null);

                    var node = cache[position];

                    // We return the value after we copy the node to the stack. 
                    Interlocked.Exchange(ref _cache, cache);

                    if (node.Type == t)
                    {
                        value = node.Value;
                        return true;
                    }

                    value = default;
                    return false;
                }
            }

            public void Put(Type t, T value)
            {
                unsafe
                {
                    // PERF: This only works because we assume that Type will not change the memory location over time.
                    // If that would not be the case we would be causing lots of cache misses, which is not the intended
                    // use of this facility. The idea is to be incredibly fast to retrieve a value if it exist in the cache
                    // even at the expense of failing to recognize that it is there.
                    TypedReference reference = __makeref(t);
                    ulong extremelyUnsafeAddress = **(ulong**)&reference;

                    var position = (int)extremelyUnsafeAddress & _andMask;

                    // We are effectively synchronizing the access to the cache because we cannot copy the whole 
                    // node to the stack in a single atomic operation. 
                    CacheItem[] cache;
                    do
                    {
                        // Because we are assigning null, noone else can continue from here. 
                        cache = Interlocked.Exchange(ref _cache, null);
                    }
                    while (cache == null);


                    // After here the cache reference will be null, effectively working as a barrier for any other
                    // attempt to set values here. 

                    // We update the copy with the new value.
                    ref var node = ref cache[position];
                    node.Type = t;
                    node.Value = value;

                    // We return the backing array after we updated the values. 
                    Interlocked.Exchange(ref _cache, cache);
                }
            }
        }

        internal class HashFastTypeCache<T>
        {
            private struct CacheItem
            {
                public Type Type;
                public T Value;
            }

            private readonly int _andMask;
            private CacheItem[] _cache;

            public HashFastTypeCache(int cacheSize = 8)
            {
                if (!Bits.IsPowerOfTwo(cacheSize))
                    cacheSize = Bits.PowerOf2(cacheSize);

                int shiftRight = Bits.CeilLog2(cacheSize);
                _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

                _cache = ArrayPool<CacheItem>.Shared.Rent(cacheSize);
            }

            public bool TryGet(Type t, out T value)
            {
                int hash = t.GetHashCode();
                var position = hash & _andMask;

                // We are effectively synchronizing the access to the cache because we cannot copy the whole 
                // node to the stack in a single atomic operation. 
                CacheItem[] cache;
                do
                {
                    // Because we are assigning null, noone else can continue from here. 
                    cache = Interlocked.Exchange(ref _cache, null);
                }
                while (cache == null);

                var node = cache[position];

                // We return the value after we copy the node to the stack. 
                Interlocked.Exchange(ref _cache, cache);

                if (node.Type == t)
                {
                    value = node.Value;
                    return true;
                }

                value = default;
                return false;
            }

            public void Set(Type t, T value)
            {
                int hash = t.GetHashCode();
                var position = hash & _andMask;

                // We are effectively synchronizing the access to the cache because we cannot copy the whole 
                // node to the stack in a single atomic operation. 
                CacheItem[] cache;
                do
                {
                    // Because we are assigning null, noone else can continue from here. 
                    cache = Interlocked.Exchange(ref _cache, null);
                }
                while (cache == null);


                // After here the cache reference will be null, effectively working as a barrier for any other
                // attempt to set values here. 

                // We update the copy with the new value.
                ref var node = ref cache[position];
                node.Type = t;
                node.Value = value;

                // We return the backing array after we updated the values. 
                Interlocked.Exchange(ref _cache, cache);
            }

            public void Dispose()
            {
                ArrayPool<CacheItem>.Shared.Return(_cache, true);
                _cache = null;
            }
        }


        internal sealed class RavenDb50TypeCache<T>
        {
            private readonly FastList<Tuple<Type, T>>[] _buckets;
            private readonly int _size;

            public RavenDb50TypeCache(int size)
            {
                _buckets = new FastList<Tuple<Type, T>>[size];
                _size = size;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGet(Type type, out T result)
            {
                int typeHash = type.GetHashCode();

                // We get the data and after that we always work from there to avoid
                // harmful race conditions.
                var storage = _buckets[typeHash % _size];
                if (storage == null)
                    goto NotFound;

                // The idea is that the type cache is big enough so that type collisions are
                // unlikely occurrences. 
                if (storage.Count != 1)
                    return TryGetUnlikely(storage, type, out result);

                ref var item = ref storage.GetAsRef(0);
                if (item.Item1 == type)
                {
                    result = item.Item2;
                    return true;
                }

                NotFound:
                result = default;
                return false;
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            public bool TryGetUnlikely(FastList<Tuple<Type, T>> storage, Type type, out T result)
            {
                for (int i = storage.Count - 1; i >= 0; i--)
                {
                    ref var item = ref storage.GetAsRef(i);
                    if (item.Item1 == type)
                    {
                        result = item.Item2;
                        return true;
                    }
                }

                result = default;
                return false;
            }

            public void Put(Type type, T value)
            {
                int typeHash = type.GetHashCode();
                int bucket = typeHash % _size;

                // The idea is that this TypeCache<T> is thread safe. It is better to lose some Put
                // that to allow side effects to happen. The tradeoff is having to recompute in case
                // of race conditions.
                FastList<Tuple<Type, T>> newBucket;
                var storage = _buckets[bucket];
                if (storage == null)
                {
                    newBucket = new(4);
                }
                else
                {
                    newBucket = new FastList<Tuple<Type, T>>(storage.Count + 1);
                    storage.CopyTo(newBucket);
                }

                newBucket.Add(new Tuple<Type, T>(type, value));
                _buckets[bucket] = newBucket;
            }
        }
    }
}
