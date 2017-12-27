using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow.Utils;
using Voron.Util;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [Config(typeof(DiffNonZeroes.Config))]
    public unsafe class DiffNonZeroes
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job(RunMode.Default)
                {
                    Env =
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

        private ByteStringContext _context;
        private int size = 1024 * 1024 * 64;
        private ByteString source;
        private ByteString modified;
        private ByteString destination;

        private ScalarDiff original;
        private DiffPages _current;
        //private AvxDiff _avx;
        //private SseDiff _sse;
        private NumericsDiff _numerics;

        

        [GlobalSetup]
        public void Setup()
        {
            _context = new ByteStringContext( SharedMultipleUseFlag.None );

            _context.Allocate(size, out source);
            _context.Allocate(size, out modified);
            _context.Allocate(size, out destination);

            var r = new Random();
            for (int i = 0; i < size; i++)
            {
                int b = r.Next();
                source.Ptr[i] = (byte)b;
                modified.Ptr[i] = (byte)b;
            }

            for (int i = 0; i < 100; i++)
            {
                int start = r.Next(size - 1000);
                int end = start + 256 + r.Next(512);

                for (;start < end; start++)
                    source.Ptr[i] = 0;
            }

            original = new ScalarDiff
            {
                OutputSize = 0,
                Output = destination.Ptr
            };

            //_avx = new AvxDiff
            //{
            //    OutputSize = 0,
            //    Output = destination.Ptr
            //};

            //_sse = new SseDiff
            //{
            //    OutputSize = 0,
            //    Output = destination.Ptr
            //};

            _numerics = new NumericsDiff
            {
                OutputSize = 0,
                Output = destination.Ptr
            };

            _current = new DiffPages
            {
                OutputSize = 0,
                Output = destination.Ptr,
            };
        }



        //[Benchmark]
        //public void Current_Sequential()
        //{
        //    _current.ComputeDiff(source.Ptr, modified.Ptr, size);
        //}

        [Benchmark]
        public void PointerOffset_Sequential()
        {
            original.ComputeDiffPointerOffset(source.Ptr, modified.Ptr, size);
        }

        //[Benchmark]
        //public void PointerOffsetWithRefs_Sequential()
        //{
        //    original.ComputeDiffPointerOffsetWithRefs(source.Ptr, modified.Ptr, size);
        //}


        //[Benchmark]
        //public void CacheAware_Sequential()
        //{
        //    original.ComputeCacheAware(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void CacheAware_MagicMult_Sequential()
        //{
        //    original.ComputeCacheAware_MagicMult(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_Blocks_Sequential()
        //{
        //    original.ComputeCacheAware_Blocks(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_BlocksInBytes_Sequential()
        //{
        //    original.ComputeCacheAware_BlocksInBytes(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_Branchless_Sequential()
        //{
        //    original.ComputeCacheAware_Branchless(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_Branchless_LessRegisters_Sequential()
        //{
        //    original.ComputeCacheAware_Branchless_LessRegisters(source.Ptr, modified.Ptr, size);
        //}


        //[Benchmark]
        //public void ComputeCacheAware_Branchless_LessRegisters_WithPrefetching_Sequential()
        //{
        //    original.ComputeCacheAware_Branchless_LessRegisters_WithPrefetching(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_NoInnerLoop_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_NoInnerLoop(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_InvertedBuffer_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_InvertedBuffer(source.Ptr, modified.Ptr, size);
        //}


        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_InvertedBuffer_Prefetch_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_InvertedBuffer_WithPrefetch(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void Numerics32_Sequential()
        //{
        //    _numerics.ComputeDiff(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse(source.Ptr, modified.Ptr, size);
        //}

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout(source.Ptr, modified.Ptr, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath(source.Ptr, modified.Ptr, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath_WithPrefetch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath_WithPrefetch(source.Ptr, modified.Ptr, size);
        }


        //[Benchmark]
        //public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse4_Layout_NoFastPath_WithPrefetch_Sequential()
        //{
        //    original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse4_Layout_NoFastPath_WithPrefetch(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void Numerics64_Sequential()
        //{
        //    _numerics.ComputeDiff2(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void Avx_Sequential()
        //{
        //    _avx.ComputeDiff(source.Ptr, modified.Ptr, size);
        //}

        //[Benchmark]
        //public void Sse_Sequential()
        //{
        //    _sse.ComputeDiff(source.Ptr, modified.Ptr, size);
        //}

        public class NumericsDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                for (long i = 0; i < len; i += 32, originalPtr += 32, modifiedPtr += 32)
                {
                    var o0 = Unsafe.Read<Vector<long>>(originalPtr);
                    var m0 = Unsafe.Read<Vector<long>>(modifiedPtr);

                    if (allZeros)
                        allZeros &= m0.Equals(Vector<long>.Zero);

                    if (!o0.Equals(m0))
                        continue;

                    if (start == i)
                    {
                        start = i + 32;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 32;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }
            }

            public void ComputeDiff2(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                for (long i = 0; i < len; i += 64, originalPtr += 64, modifiedPtr += 64)
                {                    
                    var m0 = Unsafe.Read<Vector<long>>(modifiedPtr);
                    var m1 = Unsafe.Read<Vector<long>>(modifiedPtr + 32);

                    var o0 = Unsafe.Read<Vector<long>>(originalPtr);
                    var o1 = Unsafe.Read<Vector<long>>(originalPtr + 32);

                    if (allZeros)
                        allZeros &= m0.Equals(Vector<long>.Zero) && m1.Equals(Vector<long>.Zero);

                    if (!o0.Equals(m0) || !o1.Equals(m1))
                        continue;

                    if (start == i)
                    {
                        start = i + 64;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 64;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyFullBuffer(byte* modified, int size)
            {
                // too big, no saving, just use the full modification
                OutputSize = size;
                Memory.Copy(Output, modified, size);
                IsDiff = false;
            }
        }

//        public class SseDiff
//        {
//            public byte* Output;
//            public long OutputSize;
//            public bool IsDiff { get; private set; }

//            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
//            {
//                Debug.Assert(size % 4096 == 0);
//                Debug.Assert(size % sizeof(long) == 0);

//                var len = size;
//                IsDiff = true;

//                long start = 0;
//                OutputSize = 0;
//                bool allZeros = true;

//                // This stops the JIT from accesing originalBuffer directly, as we know
//                // it is not mutable, this lowers the number of generated instructions
//                byte* originalPtr = (byte*)originalBuffer;
//                byte* modifiedPtr = (byte*)modifiedBuffer;

//                var zero = Sse2.SetZeroVector128<byte>();
//                for (long i = 0; i < len; i += 16, originalPtr += 16, modifiedPtr += 16)
//                {
//                    var o0 = Sse2.LoadVector128(originalPtr);
//                    var m0 = Sse2.LoadVector128(modifiedPtr);

//                    if (allZeros)
//                        allZeros &= Sse41.TestZ(m0, zero);

//                    if (!Sse41.TestZ(o0, m0))
//                        continue;

//                    if (start == i)
//                    {
//                        start = i + 16;
//                        continue;
//                    }

//                    long count = (i - start) * sizeof(long);

//                    if (allZeros)
//                    {
//                        WriteDiffAllZeroes(start * sizeof(long), count);
//                    }
//                    else
//                    {
//                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
//                        allZeros = true;
//                    }

//                    start = i + 16;
//                }

//                if (start == len)
//                    return;

//                long length = (len - start) * sizeof(long);

//                if (allZeros)
//                {
//                    WriteDiffAllZeroes(start * sizeof(long), length);
//                }
//                else
//                {
//                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
//                }

//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void WriteDiffNonZeroes(long start, long count, byte* modified)
//            {
//                Debug.Assert(count > 0);
//                Debug.Assert((OutputSize % sizeof(long)) == 0);

//                long outputSize = OutputSize;
//                long* outputPtr = (long*)Output + outputSize / sizeof(long);
//                outputPtr[0] = start;
//                outputPtr[1] = count;
//                outputSize += sizeof(long) * 2;

//                Memory.Copy(Output + outputSize, modified + start, count);
//                OutputSize = outputSize + count;
//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void WriteDiffAllZeroes(long start, long count)
//            {
//                Debug.Assert(count > 0);
//                Debug.Assert((OutputSize % sizeof(long)) == 0);

//                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
//                outputPtr[0] = start;
//                outputPtr[1] = -count;

//                OutputSize += sizeof(long) * 2;
//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void CopyFullBuffer(byte* modified, int size)
//            {
//                // too big, no saving, just use the full modification
//                OutputSize = size;
//                Memory.Copy(Output, modified, size);
//                IsDiff = false;
//            }
//        }

//        public class AvxDiff
//        {
//            public byte* Output;
//            public long OutputSize;
//            public bool IsDiff { get; private set; }

//            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
//            {
//                Debug.Assert(size % 4096 == 0);
//                Debug.Assert(size % sizeof(long) == 0);

//                var len = size;
//                IsDiff = true;

//                long start = 0;
//                OutputSize = 0;
//                bool allZeros = true;

//                // This stops the JIT from accesing originalBuffer directly, as we know
//                // it is not mutable, this lowers the number of generated instructions
//                byte* originalPtr = (byte*)originalBuffer;
//                byte* modifiedPtr = (byte*)modifiedBuffer;

//                var zero = Avx.SetZeroVector256<byte>();
//                for (long i = 0; i < len; i += 32, originalPtr += 32, modifiedPtr += 32)
//                {
//                    var o0 = Avx.LoadVector256(originalPtr);
//                    var m0 = Avx.LoadVector256(modifiedPtr);

//                    if (allZeros)
//                        allZeros &= Avx.TestZ(m0, zero);

//                    if (!Avx.TestZ(o0, m0))
//                        continue;

//                    if (start == i)
//                    {
//                        start = i + 32;
//                        continue;
//                    }

//                    long count = (i - start) * sizeof(long);

//                    if (allZeros)
//                    {
//                        WriteDiffAllZeroes(start * sizeof(long), count);
//                    }
//                    else
//                    {
//                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
//                        allZeros = true;
//                    }

//                    start = i + 32;
//                }

//                if (start == len)
//                    return;

//                long length = (len - start) * sizeof(long);

//                if (allZeros)
//                {
//                    WriteDiffAllZeroes(start * sizeof(long), length);
//                }
//                else
//                {
//                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
//                }
//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void WriteDiffNonZeroes(long start, long count, byte* modified)
//            {
//                Debug.Assert(count > 0);
//                Debug.Assert((OutputSize % sizeof(long)) == 0);

//                long outputSize = OutputSize;
//                long* outputPtr = (long*)Output + outputSize / sizeof(long);
//                outputPtr[0] = start;
//                outputPtr[1] = count;
//                outputSize += sizeof(long) * 2;

//                Memory.Copy(Output + outputSize, modified + start, count);
//                OutputSize = outputSize + count;
//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void WriteDiffAllZeroes(long start, long count)
//            {
//                Debug.Assert(count > 0);
//                Debug.Assert((OutputSize % sizeof(long)) == 0);

//                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
//                outputPtr[0] = start;
//                outputPtr[1] = -count;

//                OutputSize += sizeof(long) * 2;
//            }

//            [MethodImpl(MethodImplOptions.AggressiveInlining)]
//            private void CopyFullBuffer(byte* modified, int size)
//            {
//                // too big, no saving, just use the full modification
//                OutputSize = size;
//                Memory.Copy(Output, modified, size);
//                IsDiff = false;
//            }
//        }        

        public unsafe class ScalarDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size / sizeof(long);
                IsDiff = true;

                long start = 0;
                OutputSize = 0;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                long* originalPtr = (long*)originalBuffer;
                long* modifiedPtr = (long*)modifiedBuffer;

                for (long i = 0; i < len; i += 4, originalPtr += 4, modifiedPtr += 4)
                {
                    long m0 = modifiedPtr[0];
                    long o0 = originalPtr[0];

                    long m1 = modifiedPtr[1];
                    long o1 = originalPtr[1];

                    long m2 = modifiedPtr[2];
                    long o2 = originalPtr[2];

                    long m3 = modifiedPtr[3];
                    long o3 = originalPtr[3];

                    if (o0 != m0 || o1 != m1 || o2 != m2 || o3 != m3)
                        continue;

                    if (start == i)
                    {
                        start = i + 4;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);

                    start = i + 4;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);

                WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
            }

            public void ComputeDiffPointerOffset(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size / sizeof(long);
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long* ptr = (long*)originalBuffer;
                long offset = (long*)modifiedBuffer - (long*)originalBuffer;

                for (long i = 0; i < len; i += 4, ptr += 4)
                {
                    long m0 = *(ptr + offset + 0);
                    long o0 = *(ptr + 0);

                    if (allZeros)
                        allZeros &= m0 == 0 && *(ptr + offset + 1) == 0 && *(ptr + offset + 2) == 0 && *(ptr + offset + 3) == 0;

                    if (o0 != m0 || *(ptr + 1) != *(ptr + offset + 1) || *(ptr + 2) != *(ptr + offset + 2) || *(ptr + 3) != *(ptr + offset + 3))
                        continue;

                    if (start == i)
                    {
                        start = i + 4;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)(ptr + offset));
                        allZeros = true;
                    }

                    start = i + 4;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)(ptr + offset));
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)( Output + outputSize );
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)(Output + OutputSize);
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            public void ComputeDiffPointerOffsetWithRefs(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size / sizeof(long);
                IsDiff = true;

                long start = 0;
                long outputSize = 0;
                bool allZeros = true;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long* ptr = (long*)originalBuffer;
                long offset = (long*)modifiedBuffer - (long*)originalBuffer;
                byte* output = Output;

                for (long i = 0; i < len; i += 4, ptr += 4)
                {
                    long m0 = *(ptr + offset + 0);
                    long o0 = *(ptr + 0);

                    if (allZeros)
                        allZeros &= m0 == 0 && *(ptr + offset + 1) == 0 && *(ptr + offset + 2) == 0 && *(ptr + offset + 3) == 0;

                    if (o0 != m0 || *(ptr + 1) != *(ptr + offset + 1) || *(ptr + 2) != *(ptr + offset + 2) || *(ptr + 3) != *(ptr + offset + 3))
                        continue;

                    if (start == i)
                    {
                        start = i + 4;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count, output, ref outputSize);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)(ptr + offset), output, ref outputSize);
                        allZeros = true;
                    }

                    start = i + 4;
                }

                if (start == len)
                    goto Return;

                long length = (len - start) * sizeof(long);

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length, output, ref outputSize);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)(ptr + offset), output, ref outputSize);
                }

Return:
                OutputSize = outputSize;

            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void WriteDiffNonZeroes(long start, long count, byte* modified, byte* output, ref long outputSize)
            {
                Debug.Assert(count > 0);
                Debug.Assert((outputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)(output + outputSize);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(output + outputSize, modified + start, count);
                outputSize += count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void WriteDiffAllZeroes(long start, long count, byte* output, ref long outputSize)
            {
                Debug.Assert(count > 0);
                Debug.Assert((outputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)(output + outputSize);
                outputPtr[0] = start;
                outputPtr[1] = -count;

                outputSize += sizeof(long) * 2;
            }


            public void ComputeCacheAware(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                const int pageSize = 4096;
                const int cacheBlocks = 512;
                const int blocksCount = pageSize / cacheBlocks;

                OutputSize = 0;
                IsDiff = true;

                int pages = size / pageSize;

                ulong* startPtr = (ulong*)originalBuffer;
                ulong* ptr = startPtr;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (long*)modifiedBuffer - (long*)originalBuffer;

                ulong* runStartPtr = null;
                ulong* runEndPtr = null;

                // For each page of 4096 bytes in size
                for (int page = 0; page < pages; page++)
                {
                    ulong st1 = 0, st2 = 0, st3 = 0, st4 = 0;

                    ulong* blockPtr = ptr;

                    // For each block of X bytes in size ( usually 512 bytes, that is 64 long values per block)
                    for (int block = 0; block < blocksCount; block++)
                    {
                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 

                        const int packetsCount = cacheBlocks / sizeof(ulong) / 4;
                        for (int packet = 0; packet < packetsCount; packet++, ptr += 4)
                        {
                            // PERF: In order to minimize latency
                            st1 <<= 4;
                            st2 <<= 4;
                            st3 <<= 4;
                            st4 <<= 4;

                            st1 |= *(ptr + 0) == *(ptr + offset + 0) ? 0ul : 1ul;
                            st2 |= *(ptr + 1) == *(ptr + offset + 1) ? 0ul : 1ul;
                            st3 |= *(ptr + 2) == *(ptr + offset + 2) ? 0ul : 1ul;
                            st4 |= *(ptr + 3) == *(ptr + offset + 3) ? 0ul : 1ul;

                            // This is an optimization that would allow us to find exact runs using some bit magic.
                            // We are not ready for that yet. 
                            // PERF: This idiom should be optimized like hell (we should just 'or' the zero flag, if not report it to Andy :D )
                            //st1 |= (*(ptr + 0) ^ *(ptr + offset + 0)) == 0 ? 0ul : 1ul;                        
                            //st2 |= (*(ptr + 1) ^ *(ptr + offset + 1)) == 0 ? 0ul : 1ul;
                            //st3 |= (*(ptr + 2) ^ *(ptr + offset + 2)) == 0 ? 0ul : 1ul;
                            //st4 |= (*(ptr + 3) ^ *(ptr + offset + 3)) == 0 ? 0ul : 1ul;
                        }

                        // PERF: We construct if from different registers to avoid the latency introduced by RAW hazards 
                        ulong blockBitmap = (st1 << 3) | (st2 << 2) | (st3 << 1) | st4;

                        if (blockBitmap == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            const int ulongsPerBlock = cacheBlocks / sizeof(ulong);
                            Debug.Assert(ulongsPerBlock % ulongsPerPacket == 0);

                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);

                            for (int i = 0; i < ulongsPerBlock / ulongsPerPacket; i += 1, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (blockBitmap & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + (i * ulongsPerPacket);
                                    runEndPtr = runStartPtr + ulongsPerPacket; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += ulongsPerPacket;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }

                        // We advance the pointer to the next block. 
                        blockPtr = ptr;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiff(ulong* start, ulong* end, ulong* srcPtr, long destOffset)
            {
                Debug.Assert(end - start > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);

                long startIdx = start - srcPtr; // The start index of the run based from the start of the page we are diffing.
                long runLengthInBytes = (end - start) * sizeof(ulong); // The run length from the start to the end.
                outputPtr[0] = startIdx * sizeof(ulong);
                outputPtr[1] = runLengthInBytes;
                outputSize += sizeof(long) * 2;

                Unsafe.CopyBlock(Output + outputSize, (srcPtr + destOffset) + startIdx, (uint)runLengthInBytes);

                OutputSize = outputSize + runLengthInBytes;
            }

            public void ComputeCacheAware_MagicMult(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                const int pageSize = 4096;
                const int cacheBlocks = 512;
                const int blocksCount = pageSize / cacheBlocks;

                OutputSize = 0;
                IsDiff = true;

                ulong* startPtr = (ulong*)originalBuffer;
                ulong* ptr = startPtr;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (long*)modifiedBuffer - (long*)originalBuffer;

                ulong* runStartPtr = null;
                ulong* runEndPtr = null;

                // PERF: size / 4096 -> size >> 12
                int pages = size >> 12;

                // For each page of 4096 bytes in size                
                for (int page = 0; page < pages; page++)
                {
                    ulong st1 = 0, st2 = 0, st3 = 0, st4 = 0;

                    ulong* blockPtr = ptr;

                    // For each block of X bytes in size ( usually 512 bytes, that is 64 long values per block)
                    for (int block = 0; block < blocksCount; block++)
                    {
                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 

                        const int packetsCount = cacheBlocks / sizeof(ulong) / 4;
                        for (int packet = 0; packet < packetsCount; packet++, ptr += 4)
                        {
                            // PERF: In order to minimize latency
                            st1 <<= 4;
                            st2 <<= 4;
                            st3 <<= 4;
                            st4 <<= 4;

                            st1 |= *(ptr + 0) == *(ptr + offset + 0) ? 0ul : 1ul;
                            st2 |= *(ptr + 1) == *(ptr + offset + 1) ? 0ul : 1ul;
                            st3 |= *(ptr + 2) == *(ptr + offset + 2) ? 0ul : 1ul;
                            st4 |= *(ptr + 3) == *(ptr + offset + 3) ? 0ul : 1ul;

                            // This is an optimization that would allow us to find exact runs using some bit magic.
                            // We are not ready for that yet. 
                            // PERF: This idiom should be optimized like hell (we should just 'or' the zero flag, if not report it to Andy :D )
                            //st1 |= (*(ptr + 0) ^ *(ptr + offset + 0)) == 0 ? 0ul : 1ul;                        
                            //st2 |= (*(ptr + 1) ^ *(ptr + offset + 1)) == 0 ? 0ul : 1ul;
                            //st3 |= (*(ptr + 2) ^ *(ptr + offset + 2)) == 0 ? 0ul : 1ul;
                            //st4 |= (*(ptr + 3) ^ *(ptr + offset + 3)) == 0 ? 0ul : 1ul;
                        }

                        // PERF: We construct if from different registers to avoid the latency introduced by RAW hazards 
                        ulong blockBitmap = (st1 << 3) | (st2 << 2) | (st3 << 1) | st4;

                        if (blockBitmap == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff_NoInline(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            const int ulongsPerBlock = cacheBlocks / sizeof(ulong);
                            Debug.Assert(ulongsPerBlock % ulongsPerPacket == 0);

                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);

                            for (int i = 0; i < ulongsPerBlock; i += ulongsPerPacket, mask >>= ulongsPerPacket)
                            {
                                if (runStartPtr == null)
                                {
                                    if ((blockBitmap & mask) == 0)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + i;
                                    runEndPtr = runStartPtr + ulongsPerPacket; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if ((blockBitmap & mask) != 0)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += ulongsPerPacket;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff_NoInline(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }

                        // We advance the pointer to the next block. 
                        blockPtr = ptr;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff_NoInline(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                }
            }

            private void WriteDiff_NoInline(ulong* start, ulong* end, ulong* srcPtr, long destOffset)
            {
                Debug.Assert(end - start > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);

                long startIdx = start - srcPtr; // The start index of the run based from the start of the page we are diffing.
                long runLengthInBytes = (end - start) * sizeof(ulong); // The run length from the start to the end.
                outputPtr[0] = startIdx * sizeof(ulong);
                outputPtr[1] = runLengthInBytes;
                outputSize += sizeof(long) * 2;

                Unsafe.CopyBlock(Output + outputSize, (srcPtr + destOffset) + startIdx, (uint)runLengthInBytes);

                OutputSize = outputSize + runLengthInBytes;
            }

            public void ComputeCacheAware_Blocks(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                const int pageSize = 4096;
                const int cacheBlocks = 512;

                OutputSize = 0;
                IsDiff = true;

                int pages = size / pageSize;

                ulong* startPtr = (ulong*)originalBuffer;
                ulong* ptr = startPtr;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (long*)modifiedBuffer - (long*)originalBuffer;

                ulong* runStartPtr = null;
                ulong* runEndPtr = null;

                ulong* blockBitmaps = stackalloc ulong[8];

                // These will get rotated completed on each iteration, therefore no need to reinitialize them
                ulong st1 = 0, st2 = 0, st3 = 0, st4 = 0;

                // For each page of 4096 bytes in size
                for (int page = 0; page < pages; page++)
                {
                    // The start of the block.
                    ulong* blockPtr = ptr;

                    // For each block of X bytes in size (usually 512 bytes, that is 64 long values per block)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++)
                    {
                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 
                        const int packetsCount = cacheBlocks / sizeof(ulong) / 4;
                        for (int packet = 0; packet < packetsCount; packet++, ptr += 4)
                        {
                            st1 <<= 4;
                            st2 <<= 4;
                            st3 <<= 4;
                            st4 <<= 4;

                            st1 |= *(ptr + 0) == *(ptr + offset + 0) ? 0ul : 1ul;
                            st2 |= *(ptr + 1) == *(ptr + offset + 1) ? 0ul : 1ul;
                            st3 |= *(ptr + 2) == *(ptr + offset + 2) ? 0ul : 1ul;
                            st4 |= *(ptr + 3) == *(ptr + offset + 3) ? 0ul : 1ul;

                            // This is an optimization that would allow us to find exact runs using some bit magic.
                            // We are not ready for that yet. 
                            // PERF: This idiom should be optimized like hell (we should just 'or' the zero flag, if not report it to Andy :D )
                            //st1 |= (*(ptr + 0) ^ *(ptr + offset + 0)) == 0 ? 0ul : 1ul;                        
                            //st2 |= (*(ptr + 1) ^ *(ptr + offset + 1)) == 0 ? 0ul : 1ul;
                            //st3 |= (*(ptr + 2) ^ *(ptr + offset + 2)) == 0 ? 0ul : 1ul;
                            //st4 |= (*(ptr + 3) ^ *(ptr + offset + 3)) == 0 ? 0ul : 1ul;
                        }

                        // PERF: We construct if from different registers to avoid the latency introduced by RAW hazards 
                        blockBitmaps[blockIdx] = (st1 << 3) | (st2 << 2) | (st3 << 1) | st4;
                    }

                    for (int blockIdx = 0; blockIdx < 8; blockIdx++, blockPtr += cacheBlocks / sizeof(ulong))
                    {
                        // Every iteration we advance the block pointer to the start of the next block. 

                        if (blockBitmaps[blockIdx] == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            const int ulongsPerBlock = cacheBlocks / sizeof(ulong);
                            Debug.Assert(ulongsPerBlock % ulongsPerPacket == 0);

                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);

                            for (int i = 0; i < ulongsPerBlock / ulongsPerPacket; i += 1, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (blockBitmaps[blockIdx] & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + (i * ulongsPerPacket);
                                    runEndPtr = runStartPtr + ulongsPerPacket; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += ulongsPerPacket;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, startPtr, offset); // TODO: Handle all zeroes case. 
                }
            }


            public void ComputeCacheAware_BlocksInBytes(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                OutputSize = 0;
                IsDiff = true;

                int pages = size / 4096;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                byte* runStartPtr = null;
                byte* runEndPtr = null;

                ulong* blockBitmaps = stackalloc ulong[8];

                // These will get rotated completed on each iteration, therefore no need to reinitialize them
                ulong st1 = 0, st2 = 0, st3 = 0, st4 = 0;

                byte* ptr = (byte*)originalBuffer;

                // For each page of 4096 bytes in size (that is 8 blocks of 512 bytes)
                for (int page = 0; page < pages; page++)
                {
                    // The start of the block.
                    byte* blockPtr = ptr;

                    // For each block of 512 bytes in size (that is 64 long values per block)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++)
                    {
                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 
                        for (byte* end = ptr + 512; ptr < end; ptr += 32)
                        {
                            st1 <<= 4;
                            st2 <<= 4;
                            st3 <<= 4;
                            st4 <<= 4;

                            st1 |= *(ulong*)(ptr + 0) == *(ulong*)(ptr + offset + 0) ? 0ul : 1ul;
                            st2 |= *(ulong*)(ptr + 8) == *(ulong*)(ptr + offset + 8) ? 0ul : 1ul;
                            st3 |= *(ulong*)(ptr + 16) == *(ulong*)(ptr + offset + 16) ? 0ul : 1ul;
                            st4 |= *(ulong*)(ptr + 24) == *(ulong*)(ptr + offset + 24) ? 0ul : 1ul;
                        }

                        // PERF: We construct if from different registers to avoid the latency introduced by RAW hazards 
                        blockBitmaps[blockIdx] = (st1 << 3) | (st2 << 2) | (st3 << 1) | st4;
                    }

                    // For each block of 512 bytes (or 64 long values)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++, blockPtr += 512)
                    {
                        // Every iteration we advance the block pointer to the start of the next block. 

                        if (blockBitmaps[blockIdx] == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            Debug.Assert(64 % ulongsPerPacket == 0);

                            // Ulongs processed simultaneosly: 4 therefore the mask is 0000 1111
                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);
                            ulong bitmap = blockBitmaps[blockIdx];

                            // For each packet of 512 bytes, test 32 bytes at a time 
                            for (int i = 0; i < 512; i += 32, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (bitmap & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + i;
                                    runEndPtr = runStartPtr + 32; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += 32;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                }
            }

            private void WriteDiff(byte* start, byte* end, byte* srcPtr, long destOffset)
            {
                Debug.Assert(end - start > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                byte* outputPtr = Output + OutputSize;

                long startIdx = start - srcPtr; // The start index of the run based from the start of the page we are diffing.
                long runLengthInBytes = end - start; // The run length from the start to the end.

                *(long*)(outputPtr + 0) = startIdx;
                *(long*)(outputPtr + 8) = runLengthInBytes;

                Unsafe.CopyBlock(outputPtr + 16, srcPtr + (destOffset + startIdx), (uint)runLengthInBytes);

                OutputSize += runLengthInBytes + 16;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static ulong ToUnsignedLong(bool value)
            {
                var result = Unsafe.As<bool, byte>(ref value);
                return result;
            }

            public void ComputeCacheAware_Branchless(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                OutputSize = 0;
                IsDiff = true;

                int pages = size / 4096;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                byte* runStartPtr = null;
                byte* runEndPtr = null;

                ulong* blockBitmaps = stackalloc ulong[8];

                // These will get rotated completed on each iteration, therefore no need to reinitialize them
                ulong st1 = 0, st2 = 0, st3 = 0, st4 = 0;

                byte* ptr = (byte*)originalBuffer;

                // For each page of 4096 bytes in size (that is 8 blocks of 512 bytes)
                for (int page = 0; page < pages; page++)
                {
                    // The start of the block.
                    byte* blockPtr = ptr;

                    // For each block of 512 bytes in size (that is 64 long values per block)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++)
                    {
                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 
                        for (byte* end = ptr + 512; ptr < end; ptr += 32)
                        {
                            st1 <<= 4;
                            st2 <<= 4;
                            st3 <<= 4;
                            st4 <<= 4;

                            st1 |= ToUnsignedLong(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0));
                            st2 |= ToUnsignedLong(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8));
                            st3 |= ToUnsignedLong(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16));
                            st4 |= ToUnsignedLong(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24));
                        }

                        // PERF: We construct if from different registers to avoid the latency introduced by RAW hazards 
                        blockBitmaps[blockIdx] = (st1 << 3) | (st2 << 2) | (st3 << 1) | st4;
                    }

                    // For each block of 512 bytes (or 64 long values)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++, blockPtr += 512)
                    {
                        // Every iteration we advance the block pointer to the start of the next block. 

                        if (blockBitmaps[blockIdx] == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            Debug.Assert(64 % ulongsPerPacket == 0);

                            // Ulongs processed simultaneosly: 4 therefore the mask is 0000 1111
                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);
                            ulong bitmap = blockBitmaps[blockIdx];

                            // For each packet of 512 bytes, test 32 bytes at a time 
                            for (int i = 0; i < 512; i += 32, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (bitmap & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + i;
                                    runEndPtr = runStartPtr + 32; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += 32;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static byte ToByte(bool value)
            {
                return Unsafe.As<bool, byte>(ref value);
            }

            public void ComputeCacheAware_Branchless_LessRegisters(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                OutputSize = 0;
                IsDiff = true;

                int pages = size / 4096;

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                Sse.Prefetch2(ptr);
                Sse.Prefetch2(ptr + offset);

                byte* runStartPtr = null;
                byte* runEndPtr = null;

                // These will get rotated completed on each iteration, therefore no need to reinitialize them
                ulong* blockBitmaps = stackalloc ulong[8];

                // For each page of 4096 bytes in size (that is 8 blocks of 512 bytes)
                for (int page = 0; page < pages; page++)
                {
                    // The start of the block.
                    byte* blockPtr = ptr;

                    // For each block of 512 bytes in size (that is 64 long values per block)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++)
                    {
                        ulong blockBitmap = 0;

                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 
                        for (byte* end = ptr + 512; ptr < end; ptr += 32)
                        {
                            blockBitmap = blockBitmap << 4 |
                                          (ulong)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                                  ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                                  ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                                  ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));
                        }

                        blockBitmaps[blockIdx] = blockBitmap;
                    }

                    // For each block of 512 bytes (or 64 long values)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++, blockPtr += 512)
                    {
                        // Every iteration we advance the block pointer to the start of the next block. 

                        if (blockBitmaps[blockIdx] == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            Debug.Assert(64 % ulongsPerPacket == 0);

                            // Ulongs processed simultaneosly: 4 therefore the mask is 0000 1111
                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);
                            ulong bitmap = blockBitmaps[blockIdx];

                            // For each packet of 512 bytes, test 32 bytes at a time 
                            for (int i = 0; i < 512; i += 32, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (bitmap & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + i;
                                    runEndPtr = runStartPtr + 32; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += 32;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                }
            }

            public void ComputeCacheAware_Branchless_LessRegisters_WithPrefetching(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                OutputSize = 0;
                IsDiff = true;

                int pages = size / 4096;

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register. 
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                Sse.Prefetch2(ptr);
                Sse.Prefetch2(ptr + offset);

                byte* runStartPtr = null;
                byte* runEndPtr = null;

                // These will get rotated completed on each iteration, therefore no need to reinitialize them
                ulong* blockBitmaps = stackalloc ulong[8];

                // For each page of 4096 bytes in size (that is 8 blocks of 512 bytes)
                for (int page = 0; page < pages; page++)
                {
                    // The start of the block.
                    byte* blockPtr = ptr;

                    Sse.Prefetch0(blockPtr);
                    Sse.Prefetch0(blockPtr + 256);
                    Sse.Prefetch0(blockPtr + 512);
                    Sse.Prefetch0(blockPtr + offset);
                    Sse.Prefetch0(blockPtr + offset + 256);
                    Sse.Prefetch0(blockPtr + offset + 512);

                    // For each block of 512 bytes in size (that is 64 long values per block)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++)
                    {
                        ulong blockBitmap = 0;

                        // PERF: We should prefetch here the X bytes when PREFETCH is available. 
                        for (byte* end = ptr + 512; ptr < end; ptr += 32)
                        {
                            blockBitmap = blockBitmap << 4 |
                                          (ulong)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                                  ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                                  ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                                  ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));
                        }

                        blockBitmaps[blockIdx] = blockBitmap;
                    }

                    // For each block of 512 bytes (or 64 long values)
                    for (int blockIdx = 0; blockIdx < 8; blockIdx++, blockPtr += 512)
                    {
                        // Every iteration we advance the block pointer to the start of the next block. 

                        if (blockBitmaps[blockIdx] == 0)
                        {
                            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                            if (runStartPtr != null)
                            {
                                // We have a run and we are looking to the start of a hole.
                                WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                runStartPtr = null;
                                runEndPtr = null;
                            }
                        }
                        else
                        {
                            const int ulongsPerPacket = 4;
                            Debug.Assert(64 % ulongsPerPacket == 0);

                            // Ulongs processed simultaneosly: 4 therefore the mask is 0000 1111
                            ulong mask = 0x0Ful << (64 - ulongsPerPacket);
                            ulong bitmap = blockBitmaps[blockIdx];

                            // For each packet of 512 bytes, test 32 bytes at a time 
                            for (int i = 0; i < 512; i += 32, mask >>= ulongsPerPacket)
                            {
                                bool isUntouched = (bitmap & mask) == 0;
                                if (runStartPtr == null)
                                {
                                    if (isUntouched)
                                        continue; // We dont have a run, therefore we go to the next.

                                    runStartPtr = blockPtr + i;
                                    runEndPtr = runStartPtr + 32; // TODO: Check we are using 'ptr indirect addressing' on runEndPtr
                                }
                                else // if (runPtr != null)                                
                                {
                                    if (!isUntouched)
                                    {
                                        // Add the 4 ulong that had been tested to the run. 
                                        runEndPtr += 32;
                                    }
                                    else
                                    {
                                        // We have a run and we are looking to the start of a hole.
                                        WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                                        runStartPtr = null;
                                        runEndPtr = null;
                                    }
                                }
                            }
                        }
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (runStartPtr != null)
                {
                    // We have a run and we are looking to the start of a hole.
                    WriteDiff(runStartPtr, runEndPtr, (byte*)originalBuffer, offset); // TODO: Handle all zeroes case. 
                }
            }


            public void ComputeCacheAware_SingleBody(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                int pages = size / 4096;

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                byte* writePtr = Output;
                long writePtrOffset = 16;
                long writeEndOffset = size - 128;

                bool started = false;

                // For each page of 4096 bytes in size (that is 8 blocks of 512 bytes)
                for (int page = 0; page < pages; page++)
                {
                    // For each block of 32 bytes in size (that is 4 ulong values per block)
                    for (byte* end = ptr + 4096; ptr < end; ptr += 32)
                    {
                        byte blockBitmap = (byte)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                                  ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                                  ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                                  ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));


                        if (blockBitmap != 0)
                        {
                            if (started == false)
                            {
                                // Write the start index of the run based from the start of the page we are diffing.
                                *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;

                                started = true;
                            }

                            // Execute a write on the current offset pointer. 
                            *(ulong*)(writePtr + writePtrOffset + 0) = *(ulong*)(ptr + offset + 0);
                            *(ulong*)(writePtr + writePtrOffset + 8) = *(ulong*)(ptr + offset + 8);
                            *(ulong*)(writePtr + writePtrOffset + 16) = *(ulong*)(ptr + offset + 16);
                            *(ulong*)(writePtr + writePtrOffset + 24) = *(ulong*)(ptr + offset + 24);

                            writePtrOffset += 32;
                        }
                        else if (started) // our block is untouched here. 
                        {
                            // We write the actual size of the stored data.
                            *(long*)(writePtr + 8) = writePtrOffset - 16;

                            // We advance the write pointer to the start of the next.
                            writePtr += writePtrOffset;

                            // We reset the write pointer but not before actually substracting the written amount 
                            // from the available space.
                            writeEndOffset -= writePtrOffset;
                            writePtrOffset = 16;

                            started = false;
                        }

                        // This is an optimization that would allow us to find exact runs using some bit magic.
                        // We are not ready for that yet. 
                        // PERF: This idiom should be optimized like hell (we should just 'or' the zero flag, if not report it to Andy :D )
                        //st1 |= (*(ptr + 0) ^ *(ptr + offset + 0)) == 0 ? 0ul : 1ul;                        
                        //st2 |= (*(ptr + 1) ^ *(ptr + offset + 1)) == 0 ? 0ul : 1ul;
                        //st3 |= (*(ptr + 2) ^ *(ptr + offset + 2)) == 0 ? 0ul : 1ul;
                        //st4 |= (*(ptr + 3) ^ *(ptr + offset + 3)) == 0 ? 0ul : 1ul;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;
                    writePtr += writePtrOffset;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }



            [StructLayout(LayoutKind.Explicit)]
            private struct Range
            {
                [FieldOffset(0)]
                public long Start;

                [FieldOffset(8)]
                public long Count;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    byte blockBitmap = (byte)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                              ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                              ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                              ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));

                    if (blockBitmap != 0)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            rangePtr->Start = ptr - (byte*)originalBuffer;
                            started = true;
                        }
                    }
                    else if (started) // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;

                        // We prepare for the next range. 
                        rangePtr--;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    bool blockEquals;
                    if (*(ulong*)(ptr + 0) == *(ulong*)(ptr + offset + 0))
                    {
                        var o0 = Unsafe.Read<Vector<ulong>>(ptr);
                        var m0 = Unsafe.Read<Vector<ulong>>(ptr + offset);

                        blockEquals = o0.Equals(m0);
                        if (blockEquals && !started)
                            continue;
                    }
                    else
                    {
                        blockEquals = false;
                    }

                    if (!blockEquals)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            rangePtr->Start = ptr - (byte*)originalBuffer;
                            started = true;
                        }
                    }
                    else // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;

                        // We prepare for the next range. 
                        rangePtr--;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse4_Layout_NoFastPath_WithPrefetch(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                Sse.Prefetch2(ptr);
                Sse.Prefetch2(ptr + 2048);
                Sse.Prefetch2(ptr + 4096);
                Sse.Prefetch2(ptr + 4096 + 2048);
                Sse.Prefetch2(ptr + offset);
                Sse.Prefetch2(ptr + offset + 2048);
                Sse.Prefetch2(ptr + offset + 4096);
                Sse.Prefetch2(ptr + offset + 4096 + 2048);

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    Sse.Prefetch0(ptr + 512);
                    Sse.Prefetch0(ptr + offset + 512);

                    var o0 = Sse2.LoadVector128(ptr);
                    var m0 = Sse2.LoadVector128(ptr + offset);

                    if (Sse41.TestZ(o0, m0))
                    {
                        if (!started)
                            continue;

                        goto CloseBlock;
                    }

                    // We are opening a block.
                    if (started == false)
                    {
                        // Write the start index of the run based from the start of the page we are diffing.
                        rangePtr->Start = ptr - (byte*)originalBuffer;
                        started = true;
                    }
                    continue;

                    CloseBlock:
                    // We write the actual size of the stored data.
                    rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;
                    // We prepare for the next range. 
                    rangePtr--;

                    started = false;
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath_WithPrefetch(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                Sse.Prefetch2(ptr);
                Sse.Prefetch2(ptr + 2048);
                Sse.Prefetch2(ptr + 4096);
                Sse.Prefetch2(ptr + 4096 + 2048);
                Sse.Prefetch2(ptr + offset );
                Sse.Prefetch2(ptr + offset + 2048);
                Sse.Prefetch2(ptr + offset + 4096);
                Sse.Prefetch2(ptr + offset + 4096 + 2048);

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    Sse.Prefetch0(ptr + 512);
                    Sse.Prefetch0(ptr + offset + 512);

                    var o0 = Unsafe.Read<Vector<ulong>>(ptr);
                    var m0 = Unsafe.Read<Vector<ulong>>(ptr + offset);

                    if (o0.Equals(m0))
                    {
                        if (!started)
                            continue;

                        goto CloseBlock;
                    }

                    // We are opening a block.
                    if (started == false)
                    {
                        // Write the start index of the run based from the start of the page we are diffing.
                        rangePtr->Start = ptr - (byte*)originalBuffer;
                        started = true;
                    }
                    continue;

                    CloseBlock:
                    // We write the actual size of the stored data.
                    rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;
                    // We prepare for the next range. 
                    rangePtr--;

                    started = false;
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    var o0 = Unsafe.Read<Vector<ulong>>(ptr);
                    var m0 = Unsafe.Read<Vector<ulong>>(ptr + offset);

                    if (o0.Equals(m0))
                    {
                        if (!started)
                            continue;

                        goto CloseBlock;
                    }

                    // We are opening a block.
                    if (started == false)
                    {
                        // Write the start index of the run based from the start of the page we are diffing.
                        rangePtr->Start = ptr - (byte*)originalBuffer;
                        started = true;
                    }
                    continue;

                    CloseBlock:
                    // We write the actual size of the stored data.
                    rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;
                    // We prepare for the next range. 
                    rangePtr--;

                    started = false;
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    if (*(ulong*)(ptr + 0) == *(ulong*)(ptr + offset + 0))
                    {
                        var o0 = Unsafe.Read<Vector<ulong>>(ptr);
                        var m0 = Unsafe.Read<Vector<ulong>>(ptr + offset);

                        if (o0.Equals(m0))
                        {
                            if (!started)
                                continue;

                            goto CloseBlock;
                        }
                    }

                    // We are opening a block.
                    if (started == false)
                    {
                        // Write the start index of the run based from the start of the page we are diffing.
                        rangePtr->Start = ptr - (byte*)originalBuffer;
                        started = true;
                    }
                    continue;

                    CloseBlock:
                    // We write the actual size of the stored data.
                    rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;
                    // We prepare for the next range. 
                    rangePtr--;

                    started = false;
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    bool blockEquals;
                    if (*(ulong*)(ptr + 0) == *(ulong*)(ptr + offset + 0))
                    {
                        blockEquals = *(ulong*)(ptr + 8) == *(ulong*)(ptr + offset + 8) &&
                                      *(ulong*)(ptr + 16) == *(ulong*)(ptr + offset + 16) &&
                                      *(ulong*)(ptr + 24) == *(ulong*)(ptr + offset + 24);
                    }
                    else
                    {
                        blockEquals = false;
                    }

                    if (blockEquals && !started)
                        continue;

                    if (!blockEquals)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            rangePtr->Start = ptr - (byte*)originalBuffer;
                            started = true;
                        }
                    }
                    else // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;

                        // We prepare for the next range. 
                        rangePtr--;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithPrefetch(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                // In here we will write the temporary ranges that we are going to copy. 
                Range* rangePtr = (Range*)(Output + size - sizeof(Range));
                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    Sse.Prefetch0(ptr + 128);
                    byte blockBitmap = (byte)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                              ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                              ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                              ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));

                    if (blockBitmap != 0)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            rangePtr->Start = ptr - (byte*)originalBuffer;
                            started = true;
                        }
                    }
                    else if (started) // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        rangePtr->Count = (ptr - (byte*)originalBuffer) - rangePtr->Start;

                        // We prepare for the next range. 
                        rangePtr--;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    rangePtr->Count = (long)ptr - rangePtr->Start;
                }
                else
                {
                    rangePtr++;
                }

                byte* writePtr = Output;
                for (Range* end = (Range*)(Output + size); rangePtr < end; rangePtr++)
                {
                    *((Range*)writePtr) = *rangePtr;
                    Unsafe.CopyBlock(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_NoInnerLoop(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);

                byte* ptr = (byte*)originalBuffer;

                // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
                //       hardware instructions without needed an extra register.             
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                bool started = false;

                // For each block of 32 bytes in size (that is 4 ulong values per block)
                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    var o0 = Unsafe.Read<Vector<long>>(ptr + 0);
                    var m0 = Unsafe.Read<Vector<long>>(ptr + offset + 0);

                    if (!o0.Equals(m0))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;

                            started = true;
                        }

                        // Execute a write on the current offset pointer. 
                        *(ulong*)(writePtr + writePtrOffset + 0) = *(ulong*)(ptr + offset + 0);
                        *(ulong*)(writePtr + writePtrOffset + 8) = *(ulong*)(ptr + offset + 8);
                        *(ulong*)(writePtr + writePtrOffset + 16) = *(ulong*)(ptr + offset + 16);
                        *(ulong*)(writePtr + writePtrOffset + 24) = *(ulong*)(ptr + offset + 24);

                        writePtrOffset += 32;
                    }
                    else if (started) // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        *(long*)(writePtr + 8) = writePtrOffset - 16;

                        // We advance the write pointer to the start of the next.
                        writePtr += writePtrOffset;

                        // We reset the write pointer but not before actually substracting the written amount 
                        // from the available space.
                        writePtrOffset = 16;

                        started = false;
                    }

                    // This is an optimization that would allow us to find exact runs using some bit magic.
                    // We are not ready for that yet. 
                    // PERF: This idiom should be optimized like hell (we should just 'or' the zero flag, if not report it to Andy :D )
                    //st1 |= (*(ptr + 0) ^ *(ptr + offset + 0)) == 0 ? 0ul : 1ul;                        
                    //st2 |= (*(ptr + 1) ^ *(ptr + offset + 1)) == 0 ? 0ul : 1ul;
                    //st3 |= (*(ptr + 2) ^ *(ptr + offset + 2)) == 0 ? 0ul : 1ul;
                    //st4 |= (*(ptr + 3) ^ *(ptr + offset + 3)) == 0 ? 0ul : 1ul;
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;
                    writePtr += writePtrOffset;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }
        }

        [Benchmark]
        public void Original_Sequential()
        {
            original.ComputeDiff(source.Ptr, modified.Ptr, size);
        }
    }
}
