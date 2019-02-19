using System;
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
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [DisassemblyDiagnoser]
    [Config(typeof(DiffNonZeroes.Config))]
    public unsafe class DiffNonZeroes
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

        private int size = 1024 * 1024 * 64;
        private byte* source;
        private byte* modified;
        private byte* destination;

        private ScalarDiff original;
        private DiffPages _current;

        private NumericsDiff _numerics;



        [GlobalSetup]
        public void Setup()
        {
            source = NativeMemory.AllocateMemory(size, out var _);
            modified = NativeMemory.AllocateMemory(size, out var _);
            destination = NativeMemory.AllocateMemory(size, out var _);

            var r = new Random();
            for (int i = 0; i < size; i++)
            {
                int b = r.Next();
                source[i] = (byte)b;
                modified[i] = (byte)b;
            }

            for (int i = 0; i < 100; i++)
            {
                int start = r.Next(size - 1000);
                int end = start + 256 + r.Next(4096);

                for (; start < end; start++)
                    source[start + i] = 0;
            }

            original = new ScalarDiff
            {
                OutputSize = 0,
                Output = destination
            };

            _numerics = new NumericsDiff
            {
                OutputSize = 0,
                Output = destination
            };

            _current = new DiffPages
            {
                OutputSize = 0,
                Output = destination,
            };
        }

        [Benchmark]
        public void Current_Sequential()
        {
            _current.ComputeDiff(source, modified, size);
        }

        [Benchmark]
        public void Naive_Sequential()
        {
            original.ComputeNaive_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_8bytes_Sequential()
        {
            original.ComputeNaive_8Bytes_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_16bytes_Sequential()
        {
            original.ComputeNaive_16Bytes_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_Sequential()
        {
            original.ComputeNaive_32Bytes_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_WithPrefetch_Sequential()
        {
            original.ComputeNaive_32Bytes_WithPrefetch_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_WithPrefetch_Indirect_Sequential()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_WithPrefetch_Alt_Indirect_Sequential()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Alt_Indirect_Diff(source, modified, size);
        }

        [Benchmark]
        public void ComputeWord_32Bytes_WithPrefetch_Indirect_WholeBlock_Diff()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_WholeBlock_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_WithPrefetch_Indirect_NoCount_Sequential()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_Sequential()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_Diff(source, modified, size);
        }

        [Benchmark(Baseline = true)]
        public void Naive_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_Diff()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_Diff(source, modified, size);
        }

        [Benchmark]
        public void Naive_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_NonTemporal_Diff()
        {
            original.ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_NonTemporal_Diff(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_NonTemporalRead_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_NonTemporalRead(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch_NonTemporalRead_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch_NonTemporalRead(source, modified, size);
        }

        [Benchmark]
        public void Naive_CopyBlock_Sequential()
        {
            original.ComputeNaive_CopyBlock_Diff(source, modified, size);
        }

        [Benchmark]
        public void PointerOffset_Sequential()
        {
            original.ComputeDiffPointerOffset(source, modified, size);
        }

        [Benchmark]
        public void PointerOffsetWithRefs_Sequential()
        {
            original.ComputeDiffPointerOffsetWithRefs(source, modified, size);
        }


        [Benchmark]
        public void CacheAware_Sequential()
        {
            original.ComputeCacheAware(source, modified, size);
        }

        [Benchmark]
        public void CacheAware_MagicMult_Sequential()
        {
            original.ComputeCacheAware_MagicMult(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_Blocks_Sequential()
        {
            original.ComputeCacheAware_Blocks(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_BlocksInBytes_Sequential()
        {
            original.ComputeCacheAware_BlocksInBytes(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_Branchless_Sequential()
        {
            original.ComputeCacheAware_Branchless(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_Branchless_LessRegisters_Sequential()
        {
            original.ComputeCacheAware_Branchless_LessRegisters(source, modified, size);
        }


        [Benchmark]
        public void ComputeCacheAware_Branchless_LessRegisters_WithPrefetching_Sequential()
        {
            original.ComputeCacheAware_Branchless_LessRegisters_WithPrefetching(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_Sequential()
        {
            original.ComputeCacheAware_SingleBody(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_NoInnerLoop_Numerics_Sequential()
        {
            original.ComputeCacheAware_SingleBody_NoInnerLoop_Numerics(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer(source, modified, size);
        }


        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_Prefetch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithPrefetch(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch(source, modified, size);
        }

        [Benchmark]
        public void Numerics32_Sequential()
        {
            _numerics.ComputeDiff(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse_Layout_NoFastPath(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout_NoFastPath_WithPrefetch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout_NoFastPath_WithPrefetch(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse4_Layout_NoFastPath_WithPrefetch_Sequential()
        {
            original.ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Sse4_Layout_NoFastPath_WithPrefetch(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_Avx_NonTemporal_Sequential()
        {
            original.ComputeCacheAware_SingleBody_Avx_NonTemporal(source, modified, size);
        }

        [Benchmark]
        public void ComputeCacheAware_SingleBody_Avx_Temporal_Sequential()
        {
            original.ComputeCacheAware_SingleBody_Avx_Temporal(source, modified, size);
        }


        [Benchmark]
        public void Numerics64_Sequential()
        {
            _numerics.ComputeDiff2(source, modified, size);
        }


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
                    var o0 = Memory.Read<Vector<long>>(originalPtr);
                    var m0 = Memory.Read<Vector<long>>(modifiedPtr);

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
                    var m0 = Memory.Read<Vector<long>>(modifiedPtr);
                    var m1 = Memory.Read<Vector<long>>(modifiedPtr + 32);

                    var o0 = Memory.Read<Vector<long>>(originalPtr);
                    var o1 = Memory.Read<Vector<long>>(originalPtr + 32);

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

        public unsafe class ScalarDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeNaive_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 1, originalPtr += 1, modifiedPtr += 1)
                {
                    byte m = modifiedPtr[0];
                    byte o = originalPtr[0];

                    if (m != o)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                            started = true;
                        }

                        *(writePtr + writePtrOffset) = m;
                        writePtrOffset++;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeNaive_8Bytes_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 8, originalPtr += 8, modifiedPtr += 8)
                {
                    long m = *(long*)modifiedPtr;
                    long o = *(long*)originalPtr;

                    if (m != o)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                            started = true;
                        }

                        *(long*)(writePtr + writePtrOffset) = m;
                        writePtrOffset += 8;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeNaive_16Bytes_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 16, originalPtr += 16, modifiedPtr += 16)
                {
                    Vector128<byte> m = Sse2.LoadVector128(modifiedPtr);
                    Vector128<byte> o = Sse2.LoadVector128(originalPtr);

                    o = Sse2.Xor(o, m);
                    if (!Sse41.TestZ(o, o))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                            started = true;
                        }

                        Sse2.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 16;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeNaive_32Bytes_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 32, originalPtr += 32, modifiedPtr += 32)
                {
                    Vector256<byte> m = Avx.LoadVector256(modifiedPtr);
                    Vector256<byte> o = Avx.LoadVector256(originalPtr);

                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                            started = true;
                        }

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeNaive_32Bytes_WithPrefetch_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 32, originalPtr += 32, modifiedPtr += 32)
                {
                    Sse.Prefetch0(originalPtr + 2048);
                    Sse.Prefetch0(modifiedPtr + 2048);

                    Vector256<byte> m = Avx.LoadVector256(modifiedPtr);
                    Vector256<byte> o = Avx.LoadVector256(originalPtr);

                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                            started = true;
                        }

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 32, ptr += 32)
                {
                    Sse.Prefetch0(ptr + 1024);
                    Sse.Prefetch0(ptr + offset + 1024);

                    Vector256<byte> o = Avx.LoadVector256(ptr);
                    Vector256<byte> m = Avx.LoadVector256(ptr + offset);

                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;
                            started = true;
                        }

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeWord_32Bytes_WithPrefetch_Alt_Indirect_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                for (long i = 0; i < size; i += 32, ptr += 32)
                {
                    Vector256<byte> m = Avx.LoadVector256(ptr + offset);
                    Vector256<byte> o = Avx.LoadVector256(ptr);

                    o = Avx2.Xor(o, m);
                    bool areEqual = Avx.TestZ(o, o);

                    Sse.Prefetch0(ptr + 1024);
                    Sse.Prefetch0(ptr + offset + 1024);

                    if (!areEqual)
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;
                            started = true;
                        }

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_WholeBlock_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                bool started = false;
                for (byte* end = originalPtr + size; originalPtr < end; originalPtr += 32, modifiedPtr += 32)
                {
                Top:

                    Vector256<byte> m = Avx.LoadVector256(modifiedPtr);
                    Vector256<byte> o = Avx.LoadVector256(originalPtr);

                    // Fast-Path
                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;

                        if (!started)
                            goto StartBlock;
                    }
                    else if (started)
                        goto CloseBlock;

                    Sse.Prefetch0(originalPtr + 1024);
                    Sse.Prefetch0(modifiedPtr + 1024);

                    originalPtr += 32;
                    modifiedPtr += 32;

                    if (originalPtr >= end)
                        break; // Early break. 

                    goto Top;

                CloseBlock:
                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;

                    // We advance the write pointer to the start of the next.
                    writePtr += writePtrOffset;

                    // We reset the write pointer but not before actually substracting the written amount 
                    // from the available space.
                    writePtrOffset = 16;

                    started = false;
                    continue;

                StartBlock:
                    // Write the start index of the run based from the start of the page we are diffing.
                    *(long*)(writePtr + 0) = originalPtr - (byte*)originalBuffer;
                    started = true;
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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    Sse.Prefetch0(ptr + 1024);
                    Sse.Prefetch0(ptr + offset + 1024);

                    Vector256<byte> m = Avx.LoadVector256(ptr + offset);
                    Vector256<byte> o = Avx.LoadVector256(ptr);

                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;
                            started = true;
                        }

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our byte is untouched here. 
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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                for (byte* end = ptr + size; ptr < end; ptr += 32)
                {
                    Vector256<byte> m = Avx.LoadVector256(ptr + offset);
                    Vector256<byte> o = Avx.LoadVector256(ptr);

                    o = Avx2.Xor(o, m);
                    if (Avx.TestZ(o, o))
                    {
                        if (started) // our byte is untouched here. 
                            goto CloseBlock;
                    }
                    else
                    {
                        if (started == false)
                            goto StartBlock;

                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }

                    Sse.Prefetch0(ptr + offset + 1024);
                    Sse.Prefetch0(ptr + 1024);
                    continue;

                StartBlock:
                    // Write the start index of the run based from the start of the page we are diffing.
                    *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer;
                    started = true;
                    continue;

                CloseBlock:

                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;

                    // We advance the write pointer to the start of the next.
                    writePtr += writePtrOffset;

                    // We reset the write pointer but not before actually substracting the written amount 
                    // from the available space.
                    writePtrOffset = 16;

                    started = false;
                    continue;
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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                byte* end = ptr + size;
                do
                {
                    Vector256<byte> m = Avx.LoadVector256(ptr + offset);
                    Vector256<byte> o = Avx.LoadVector256(ptr);

                    Sse.Prefetch0(ptr + offset + 1024);
                    Sse.Prefetch0(ptr + 1024);

                    ptr += 32;

                    o = Avx2.Xor(o, m);
                    if (Avx.TestZ(o, o))
                    {
                        if (started)
                            goto CloseBlock;

                        continue;
                    }

                    if (started == false)
                        goto StartBlock;

                    Avx.Store((writePtr + writePtrOffset), m);
                    writePtrOffset += 32;
                    continue;

                CloseBlock:
                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;

                    // We advance the write pointer to the start of the next.
                    writePtr += writePtrOffset;

                    // We reset the write pointer but not before actually substracting the written amount 
                    // from the available space.
                    writePtrOffset = 16;

                    started = false;
                    continue;

                StartBlock:
                    // Write the start index of the run based from the start of the page we are diffing.
                    *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer - 32;
                    started = true;
                }
                while (ptr < end);

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

            public void ComputeWord_32Bytes_WithPrefetch_Indirect_NoCount_StreamedLayout_While_NonTemporal_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;
                OutputSize = 0;

                byte* writePtr = Output;
                long writePtrOffset = 16;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* ptr = (byte*)originalBuffer;
                long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

                bool started = false;

                byte* end = ptr + size;
                do
                {
                    Vector256<byte> m = Avx.LoadAlignedVector256(ptr + offset);
                    Vector256<byte> o = Avx.LoadAlignedVector256(ptr);

                    ptr += 32;

                    Sse.Prefetch0(ptr + offset + 1024);
                    Sse.Prefetch0(ptr + 1024);

                    o = Avx2.Xor(o, m);
                    if (!Avx.TestZ(o, o))
                    {
                        if (started == false)
                            goto StartBlock;

                        //Sse2.StoreAlignedNonTemporal((writePtr + writePtrOffset), Avx.ExtractVector128(m, 0));
                        //Sse2.StoreAlignedNonTemporal((writePtr + writePtrOffset), Avx.ExtractVector128(m, 1));
                        Avx.Store((writePtr + writePtrOffset), m);
                        writePtrOffset += 32;
                    }
                    else
                    {
                        if (started) // our byte is untouched here. 
                            goto CloseBlock;
                    }

                    continue;

                CloseBlock:
                    // We write the actual size of the stored data.
                    *(long*)(writePtr + 8) = writePtrOffset - 16;

                    // We advance the write pointer to the start of the next.
                    writePtr += writePtrOffset;

                    // We reset the write pointer but not before actually substracting the written amount 
                    // from the available space.
                    writePtrOffset = 16;

                    started = false;
                    continue;

                StartBlock:
                    // Write the start index of the run based from the start of the page we are diffing.
                    *(long*)(writePtr + 0) = ptr - (byte*)originalBuffer - 32;
                    started = true;
                }
                while (ptr < end);

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

            public void ComputeNaive_CopyBlock_Diff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                IsDiff = true;

                long start = 0;
                OutputSize = 0;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                for (long i = 0; i < size; i += 1, originalPtr += 1, modifiedPtr += 1)
                {
                    byte m = modifiedPtr[0];
                    byte o = originalPtr[0];

                    if (o != m)
                        continue;

                    if (start == i)
                    {
                        start = i + 1;
                        continue;
                    }

                    long count = (i - start);

                    WriteDiffNonZeroes(start, count, (byte*)modifiedBuffer);

                    start = i + 1;
                }

                if (start == size)
                    return;

                long length = size - start;

                WriteDiffNonZeroes(start, length, (byte*)modifiedBuffer);
            }

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
            private void WriteDiffNonZeroesByte(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)(Output + outputSize);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)(Output + outputSize);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.CopyUnaligned(Output + outputSize, modified + start, (uint)count);
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

                Memory.Copy(Output + outputSize, (srcPtr + destOffset) + startIdx, (uint)runLengthInBytes);

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

                Memory.Copy(Output + outputSize, (srcPtr + destOffset) + startIdx, (uint)runLengthInBytes);

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

                Memory.Copy(outputPtr + 16, srcPtr + (destOffset + startIdx), (uint)runLengthInBytes);

                OutputSize += runLengthInBytes + 16;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static ulong ToUnsignedLong(bool value)
            {
                var result = Memory.As<bool, byte>(ref value);
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
                return Memory.As<bool, byte>(ref value);
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
#pragma warning disable 675
                            blockBitmap = blockBitmap << 4 |
                                          (ulong)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                                  ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                                  ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                                  ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));
#pragma warning restore 675
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
#pragma warning disable 675
                            blockBitmap = blockBitmap << 4 |
                                          (ulong)(ToByte(*(ulong*)(ptr + 0) != *(ulong*)(ptr + offset + 0)) << 3 |
                                                  ToByte(*(ulong*)(ptr + 8) != *(ulong*)(ptr + offset + 8)) << 2 |
                                                  ToByte(*(ulong*)(ptr + 16) != *(ulong*)(ptr + offset + 16)) << 1 |
                                                  ToByte(*(ulong*)(ptr + 24) != *(ulong*)(ptr + offset + 24)));
#pragma warning restore 675
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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics(void* originalBuffer, void* modifiedBuffer, int size)
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
                        var o0 = Memory.Read<Vector<ulong>>(ptr);
                        var m0 = Memory.Read<Vector<ulong>>(ptr + offset);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

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
                for (byte* end = ptr + size; ptr < end; ptr += 16)
                {
                    Sse.Prefetch0(ptr + 512);
                    Sse.Prefetch0(ptr + offset + 512);

                    var o0 = Sse2.LoadVector128(ptr);
                    var m0 = Sse2.LoadVector128(ptr + offset);

                    if (Sse41.TestC(o0, m0))
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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout_NoFastPath_WithPrefetch(void* originalBuffer, void* modifiedBuffer, int size)
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

                    var o0 = Memory.Read<Vector<ulong>>(ptr);
                    var m0 = Memory.Read<Vector<ulong>>(ptr + offset);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch(void* originalBuffer, void* modifiedBuffer, int size)
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

                    var o0 = Avx.LoadVector256(ptr);
                    var m0 = Avx.LoadVector256(ptr + offset);

                    if (Avx.TestC(o0, m0))
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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_NonTemporalRead(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert((long)originalBuffer % 32 == 0);
                Debug.Assert((long)modifiedBuffer % 32 == 0);
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
                    var o0 = Avx2.LoadAlignedVector256NonTemporal(ptr);
                    var m0 = Avx2.LoadAlignedVector256NonTemporal(ptr + offset);

                    if (Avx.TestC(o0, m0))
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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Avx_Layout_NoFastPath_WithPrefetch_NonTemporalRead(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert((long)originalBuffer % 32 == 0);
                Debug.Assert((long)modifiedBuffer % 32 == 0);
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
                    Sse.PrefetchNonTemporal(ptr + 512);
                    Sse.PrefetchNonTemporal(ptr + offset + 512);

                    var o0 = Avx2.LoadAlignedVector256NonTemporal(ptr);
                    var m0 = Avx2.LoadAlignedVector256NonTemporal(ptr + offset);

                    if (Avx.TestC(o0, m0))
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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

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
                    var o0 = Memory.Read<Vector<ulong>>(ptr);
                    var m0 = Memory.Read<Vector<ulong>>(ptr + offset);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_InvertedBuffer_WithBranch_Numerics_Layout(void* originalBuffer, void* modifiedBuffer, int size)
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
                        var o0 = Memory.Read<Vector<ulong>>(ptr);
                        var m0 = Memory.Read<Vector<ulong>>(ptr + offset);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

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
                    Memory.Copy(writePtr + 16, (byte*)modifiedBuffer + rangePtr->Start, (uint)rangePtr->Count);

                    writePtr += rangePtr->Count + 16;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_NoInnerLoop_Numerics(void* originalBuffer, void* modifiedBuffer, int size)
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
                    var o0 = Memory.Read<Vector<long>>(ptr + 0);
                    var m0 = Memory.Read<Vector<long>>(ptr + offset + 0);

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

            public void ComputeCacheAware_SingleBody_Avx_NonTemporal(void* originalBuffer, void* modifiedBuffer, int size)
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
                    var o = Avx.LoadVector256(ptr);
                    var m = Avx.LoadVector256(ptr + offset);

                    if (!Avx.TestC(o, m))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            Sse2.StoreNonTemporal((long*)(writePtr + 0), ptr - (byte*)originalBuffer);
                            started = true;
                        }

                        // Execute a write on the current offset pointer.    
                        byte* auxWritePtr = writePtr + writePtrOffset;
                        Sse2.StoreNonTemporal((long*)(auxWritePtr + 0), *(long*)(ptr + offset + 0));
                        Sse2.StoreNonTemporal((long*)(auxWritePtr + 16), *(long*)(ptr + offset + 16));

                        writePtrOffset += 32;
                    }
                    else if (started) // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        Sse2.StoreNonTemporal((long*)(writePtr + 8), writePtrOffset - 16);

                        // We advance the write pointer to the start of the next.
                        writePtr += writePtrOffset;

                        // We reset the write pointer but not before actually substracting the written amount 
                        // from the available space.
                        writePtrOffset = 16;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    Sse2.StoreNonTemporal((long*)(writePtr + 8), writePtrOffset - 16);
                    writePtr += writePtrOffset;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }

            public void ComputeCacheAware_SingleBody_Avx_Temporal(void* originalBuffer, void* modifiedBuffer, int size)
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
                    var o = Avx.LoadVector256(ptr);
                    var m = Avx.LoadVector256(ptr + offset);

                    if (!Avx.TestC(o, m))
                    {
                        if (started == false)
                        {
                            // Write the start index of the run based from the start of the page we are diffing.
                            Sse2.StoreNonTemporal((long*)(writePtr + 0), ptr - (byte*)originalBuffer);
                            started = true;
                        }

                        // Execute a write on the current offset pointer.    
                        Avx.Store(writePtr + writePtrOffset, m);
                        writePtrOffset += 32;
                    }
                    else if (started) // our block is untouched here. 
                    {
                        // We write the actual size of the stored data.
                        Sse2.StoreNonTemporal((long*)(writePtr + 8), writePtrOffset - 16);

                        // We advance the write pointer to the start of the next.
                        writePtr += writePtrOffset;

                        // We reset the write pointer but not before actually substracting the written amount 
                        // from the available space.
                        writePtrOffset = 16;

                        started = false;
                    }
                }

                // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
                if (started)
                {
                    // We write the actual size of the stored data.
                    Sse2.StoreNonTemporal((long*)(writePtr + 8), writePtrOffset - 16);
                    writePtr += writePtrOffset;
                }

                this.OutputSize = writePtr - Output;
                this.IsDiff = true;
            }
        }

        [Benchmark(Baseline = true)]
        public void Original_Sequential()
        {
            original.ComputeDiff(source, modified, size);
        }
    }
}
