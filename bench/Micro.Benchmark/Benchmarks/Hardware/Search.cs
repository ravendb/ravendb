using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Threading;
using Voron;
using Voron.Data.BTrees;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [Config(typeof(BinarySearching.Config))]
    public unsafe class BinarySearching
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job(RunMode.Default)
                {
                    Environment =
                    {
                        Runtime = Runtime.Core,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
                });

                // Exporters for data
                Add(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                Add(RPlotExporter.Default);

                Add(StatisticColumn.AllStatistics);

                Add(BaselineValidator.FailOnError);
                Add(JitOptimizationsValidator.FailOnError);

                Add(EnvironmentAnalyser.Default);
            }
        }

        private const int KeysToAdd = 251;

        private ByteStringContext _context;
        private int size = 1024 * 1024 * 64;

        private ByteString source;
        private TreePage[] _pages;
        private Slice[] _keys;

        private int _nextPage = 0;
        private int _nextKey = 0;

        private Random _random = new Random();

        private string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            var str = new char[length];
            for (int i = 0; i < length; i++)
            {
                str[i] = chars[_random.Next(chars.Length)];
            }
            return new string(str);
        }

        [GlobalSetup]
        public void Setup()
        {
            _context = new ByteStringContext();

            _context.Allocate(size, out source);

            int pagesCount = size / 8096;

            // We prepare multiple of those to ensure we have a cold cache when needed. 
            _pages = new TreePage[pagesCount];
            for (int i = 0; i < _pages.Length; i++)
            {
                _pages[i] = new TreePage(source.Ptr + i * 8096, 8096)
                {
                    Upper = 8096,
                    Lower = Voron.Global.Constants.Tree.PageHeaderSize,
                    TreeFlags = TreePageFlags.None,
                };
            }

            _keys = new Slice[KeysToAdd];
            var sortedRandStr = new SortedList<Slice, Slice>(SliceComparer.Instance);
            for (int index = 0; index < _keys.Length; index++)
            {
                Slice.From(_context, RandomString(19), out Slice slice);
                _keys[index] = slice;
                sortedRandStr.Add(slice, slice);
            }

            // We need to insert in order, because we need them to be in order and the pages do not care about it. 
            int position = 0;
            foreach (var key in sortedRandStr)
            {
                for (int j = 0; j < _pages.Length; j++)
                {
                    _pages[j].AddDataNode(position, key.Value, 0);
                }
                position++;
            }
        }


        [Benchmark]
        public void Original_NoCacheMisses()
        {
            var page = _pages[0];

            Original_Search(page, _context, _keys[_nextKey]);

            _nextKey = _random.Next(KeysToAdd);
        }

        [Benchmark]
        public void Original_WithCacheMisses()
        {
            Original_Search(_pages[_nextPage], _context, _keys[_nextKey]);

            _nextPage = _random.Next(_pages.Length);
            _nextKey = _random.Next(_keys.Length);
        }

        [Benchmark]
        public void Original_Prefetch_WithCacheMisses()
        {
            Original_WithPrefetch_Search(_pages[_nextPage], _context, _keys[_nextKey]);

            _nextPage = _random.Next(_pages.Length);
            _nextKey = _random.Next(_keys.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Original_Search(TreePage page, ByteStringContext allocator, Slice key)
        {
            int numberOfEntries = page.NumberOfEntries;
            if (numberOfEntries == 0)
                goto NoEntries;

            int lastMatch = -1;
            int lastSearchPosition = 0;

            SliceOptions options = key.Options;
            if (options == SliceOptions.Key)
            {
                if (numberOfEntries == 1)
                    goto SingleEntryKey;

                int low = page.IsLeaf ? 0 : 1;
                int high = numberOfEntries - 1;
                int position = 0;

                ushort* offsets = page.KeysOffsets;
                byte* @base = page.Base;
                while (low <= high)
                {
                    position = (low + high) >> 1;

                    var node = (TreeNodeHeader*)(@base + offsets[position]);

                    Slice pageKey;
                    using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                    {
                        lastMatch = SliceComparer.CompareInline(key, pageKey);
                    }

                    if (lastMatch == 0)
                        break;

                    if (lastMatch > 0)
                        low = position + 1;
                    else
                        high = position - 1;
                }

                if (lastMatch > 0) // found entry less than key
                {
                    position++; // move to the smallest entry larger than the key
                }

                lastSearchPosition = position;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.BeforeAllKeys)
            {
                lastMatch = 1;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.AfterAllKeys)
            {
                lastSearchPosition = numberOfEntries - 1;
                goto MultipleEntryKey;
            }

            return null;

        NoEntries:
            {
                page.LastSearchPosition = 0;
                page.LastMatch = 1;
                return null;
            }

        SingleEntryKey:
            {
                var node = page.GetNode(0);

                Slice pageKey;
                using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                {
                    page.LastMatch = SliceComparer.CompareInline(key, pageKey);
                }

                page.LastSearchPosition = page.LastMatch > 0 ? 1 : 0;
                return page.LastSearchPosition == 0 ? node : null;
            }

        MultipleEntryKey:
            {
                page.LastMatch = lastMatch;
                page.LastSearchPosition = lastSearchPosition;

                if (lastSearchPosition >= numberOfEntries)
                    return null;

                return page.GetNode(lastSearchPosition);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Original_WithPrefetch_Search(TreePage page, ByteStringContext allocator, Slice key)
        {
            int numberOfEntries = page.NumberOfEntries;
            if (numberOfEntries == 0)
                goto NoEntries;

            int lastMatch = -1;
            int lastSearchPosition = 0;

            SliceOptions options = key.Options;
            if (options == SliceOptions.Key)
            {
                if (numberOfEntries == 1)
                    goto SingleEntryKey;

                int low = page.IsLeaf ? 0 : 1;
                int high = numberOfEntries - 1;
                int position = 0;

                ushort* offsets = page.KeysOffsets;
                byte* @base = page.Base;
                while (low <= high)
                {
                    position = (low + high) >> 1;

                    var node = (TreeNodeHeader*)(@base + offsets[position]);

                    Slice pageKey;
                    using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                    {
                        Sse.Prefetch0(pageKey.Content.Ptr);
                        lastMatch = SliceComparer.CompareInline(key, pageKey);
                    }

                    if (lastMatch == 0)
                        break;

                    if (lastMatch > 0)
                        low = position + 1;
                    else
                        high = position - 1;
                }

                if (lastMatch > 0) // found entry less than key
                {
                    position++; // move to the smallest entry larger than the key
                }

                lastSearchPosition = position;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.BeforeAllKeys)
            {
                lastMatch = 1;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.AfterAllKeys)
            {
                lastSearchPosition = numberOfEntries - 1;
                goto MultipleEntryKey;
            }

            return null;

        NoEntries:
            {
                page.LastSearchPosition = 0;
                page.LastMatch = 1;
                return null;
            }

        SingleEntryKey:
            {
                var node = page.GetNode(0);

                Slice pageKey;
                using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                {
                    page.LastMatch = SliceComparer.CompareInline(key, pageKey);
                }

                page.LastSearchPosition = page.LastMatch > 0 ? 1 : 0;
                return page.LastSearchPosition == 0 ? node : null;
            }

        MultipleEntryKey:
            {
                page.LastMatch = lastMatch;
                page.LastSearchPosition = lastSearchPosition;

                if (lastSearchPosition >= numberOfEntries)
                    return null;

                return page.GetNode(lastSearchPosition);
            }
        }

    }
}
