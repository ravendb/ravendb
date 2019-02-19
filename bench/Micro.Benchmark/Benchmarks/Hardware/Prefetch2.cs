using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Utils;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [Config(typeof(PrefetchLayout.Config))]
    public unsafe class PrefetchLayout
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
                int end = start + 256 + r.Next(512);

                for (; start < end; start++)
                    source[i] = 0;
            }
        }

        private interface IPrefetchSize
        {
            int PrefetchStride { get; }
        }

        private interface IPrefetchConfiguration
        {
            void Prefetch(byte* ptr);
        }

        private struct Size32 : IPrefetchSize { public int PrefetchStride => 32; }
        private struct Size64 : IPrefetchSize { public int PrefetchStride => 64; }        
        private struct Size128 : IPrefetchSize { public int PrefetchStride => 128; }
        private struct Size256 : IPrefetchSize { public int PrefetchStride => 256; }
        private struct Size512 : IPrefetchSize { public int PrefetchStride => 512; }
        private struct Size1024 : IPrefetchSize { public int PrefetchStride => 1024; }
        private struct Size2048 : IPrefetchSize { public int PrefetchStride => 2048; }
        private struct Size4096 : IPrefetchSize { public int PrefetchStride => 4096; }
        private struct Size8192 : IPrefetchSize { public int PrefetchStride => 8192; }
        private struct Size16384 : IPrefetchSize { public int PrefetchStride => 16384; }

        private struct Prefetch0<TPrefetchSize> : IPrefetchConfiguration where TPrefetchSize : struct, IPrefetchSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Prefetch(byte* ptr)
            {
                TPrefetchSize prefetchConfig = default;
                Sse.Prefetch0(ptr + prefetchConfig.PrefetchStride);
            }
        }

        private struct Prefetch1<TPrefetchSize> : IPrefetchConfiguration where TPrefetchSize : struct, IPrefetchSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Prefetch(byte* ptr)
            {
                TPrefetchSize prefetchConfig = default;
                Sse.Prefetch1(ptr + prefetchConfig.PrefetchStride);
            }
        }

        private struct Prefetch2<TPrefetchSize> : IPrefetchConfiguration where TPrefetchSize : struct, IPrefetchSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Prefetch(byte* ptr)
            {
                TPrefetchSize prefetchConfig = default;
                Sse.Prefetch2(ptr + prefetchConfig.PrefetchStride);
            }
        }

        private struct PrefetchNT<TPrefetchSize> : IPrefetchConfiguration where TPrefetchSize : struct, IPrefetchSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Prefetch(byte* ptr)
            {
                TPrefetchSize prefetchConfig = default;
                Sse.PrefetchNonTemporal(ptr + prefetchConfig.PrefetchStride);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Word_32Bytes_WithPrefetch_Internal<TPrefetchConfiguration>(void* originalBuffer, void* modifiedBuffer, int size)
            where TPrefetchConfiguration : struct, IPrefetchConfiguration
        {
            Debug.Assert(size % 4096 == 0);
            Debug.Assert(size % sizeof(long) == 0);

            TPrefetchConfiguration config = default;

            byte* writePtr = destination;
            long writePtrOffset = 16;

            // This stops the JIT from accesing originalBuffer directly, as we know
            // it is not mutable, this lowers the number of generated instructions
            byte* ptr = (byte*)originalBuffer;
            long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

            bool started = false;

            for (byte* end = ptr + size; ptr < end; ptr += 32)
            {
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

                config.Prefetch(ptr + 2048);
                config.Prefetch(ptr + offset + 2048);
            }

            // If the block hasnt been touched, nothing to do here unless we have an open pointer. 
            if (started)
            {
                // We write the actual size of the stored data.
                *(long*)(writePtr + 8) = writePtrOffset - 16;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Word_32Bytes_NoPrefetch_Internal(void* originalBuffer, void* modifiedBuffer, int size)
        {
            Debug.Assert(size % 4096 == 0);
            Debug.Assert(size % sizeof(long) == 0);

            byte* writePtr = destination;
            long writePtrOffset = 16;

            // This stops the JIT from accesing originalBuffer directly, as we know
            // it is not mutable, this lowers the number of generated instructions
            byte* ptr = (byte*)originalBuffer;
            long offset = (byte*)modifiedBuffer - (byte*)originalBuffer;

            bool started = false;

            for (byte* end = ptr + size; ptr < end; ptr += 32)
            {
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
            }
        }

        [Benchmark(Baseline=true)]
        public void Word32_NoPrefetch()
        {
            this.Word_32Bytes_NoPrefetch_Internal(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_32()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size32>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_64()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size64>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_128()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size128>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_256()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size256>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_512()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size512>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_1024()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size1024>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_2048()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size2048>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_4096()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size4096>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_8192()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size8192>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch0_16384()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch0<Size16384>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_64()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size64>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_128()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size128>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_256()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size256>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_512()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size512>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_1024()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size1024>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_2048()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size2048>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_4096()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size4096>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_8192()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size8192>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch1_16384()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch1<Size16384>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_64()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size64>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_128()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size128>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_256()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size256>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_512()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size512>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_1024()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size1024>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_2048()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size2048>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_4096()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size4096>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_8192()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size8192>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_Prefetch2_16384()
        {
            this.Word_32Bytes_WithPrefetch_Internal<Prefetch2<Size16384>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_64()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size64>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_128()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size128>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_256()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size256>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_512()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size512>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_1024()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size1024>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_2048()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size2048>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_4096()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size4096>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_8192()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size8192>>(this.source, this.modified, this.size);
        }

        [Benchmark]
        public void Word32_PrefetchNT_16384()
        {
            this.Word_32Bytes_WithPrefetch_Internal<PrefetchNT<Size16384>>(this.source, this.modified, this.size);
        }
    }
}
