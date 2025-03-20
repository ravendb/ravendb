using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Binary;


namespace Micro.Benchmark.Benchmarks.Parsing
{
    //[HardwareCounters(HardwareCounter.CacheMisses, HardwareCounter.TotalIssues, HardwareCounter.BranchMispredictions, HardwareCounter.InstructionRetired )]
    [DisassemblyDiagnoser]
    [Config(typeof(FindEscapePositions.Config))]
    public unsafe class FindEscapePositions
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job(RunMode.Default)
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core80, 
                        Platform = Platform.X64, 
                        Jit = Jit.RyuJit
                    }
                });
            }
        }

        private const int MaxSize = 4096 * 64;

        [Params(7, 16, 127, 128, 255, 256, 4096, MaxSize)]        
        public int Length { get; set; }

        [Params(0.0f, 0.05f, 0.1f, 0.5f, 0.99f, 1.0f)]        
        public float NonAsciiProbability { get; set; }

        public const int Operations = 100;

        private readonly char[] _source = new char[MaxSize];

        [GlobalSetup]
        public void Setup()
        {
            var r = new Random();
            for (int i = 0; i < MaxSize; i++)
            {
                if (r.NextSingle() < NonAsciiProbability)
                {
                    _source[i] = (char)(r.Next(char.MaxValue - 128) + 128);
                }
                else
                {
                    _source[i] = (char)r.Next(128);
                }
            }
        }

        [Benchmark(Baseline = true)]
        public int Reference()
        {
            var count = 0;
            var controlCount = 0;

            for (int i = 0; i < _source.Length; i++)
            {
                var value = _source[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 10 => '\n' => 0000 1010

                // 12 => '\f' => 0000 1100
                // 13 => '\r' => 0000 1101

                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32)
                {
                    controlCount++;
                }
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }


        private static ReadOnlySpan<int> EscapePositionsCountTable =>
        [
            0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ];

        private static ReadOnlySpan<int> EscapePositionsControlTable =>
        [
            1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ];

        [Benchmark]
        public int TableBased()
        {
            var count = 0;
            var controlCount = 0;

            foreach (var value in _source)
            {
                if (value >= byte.MaxValue)
                    continue;

                count += EscapePositionsCountTable[value];
                controlCount += EscapePositionsControlTable[value];
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }

        [Benchmark]
        public int SuperScalar()
        {
            var count = 0;
            var controlCount = 0;

            const int CharsPerUlong = sizeof(ulong) / sizeof(char);  // Number of chars that fit in a word (8 bytes / 2 bytes per char)

            // Bytes covered by full 8-byte segments
            int fullSegmentsBytes = (_source.Length / CharsPerUlong) * sizeof(ulong);

            // Get a reference to the string as bytes
            ref byte strBytes = ref Unsafe.As<char, byte>(ref MemoryMarshal.GetReference<char>(_source));

            // Process full 8-byte segments using byte offsets
            for (nint byteOffset = 0; byteOffset < fullSegmentsBytes; byteOffset += sizeof(ulong))
            {
                // Read 8 bytes at the current offset
                ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref strBytes, byteOffset));

                ulong unicodeHighBytes = value & 0xFF00FF00FF00FF00;
                if (unicodeHighBytes == 0)
                {
                    // All chars < 255; process in parallel
                    byte b0 = (byte)value;          // Low byte of char 0
                    byte b1 = (byte)(value >> 1 * Bits.InChar);  // Low byte of char 1
                    byte b2 = (byte)(value >> 2 * Bits.InChar);  // Low byte of char 2
                    byte b3 = (byte)(value >> 3 * Bits.InChar);  // Low byte of char 3

                    count += EscapePositionsCountTable[b0];
                    count += EscapePositionsCountTable[b1];
                    count += EscapePositionsCountTable[b2];
                    count += EscapePositionsCountTable[b3];

                    controlCount += EscapePositionsControlTable[b0];
                    controlCount += EscapePositionsControlTable[b1];
                    controlCount += EscapePositionsControlTable[b2];
                    controlCount += EscapePositionsControlTable[b3];
                }
                else
                {

                    // At least one char >= 255; process each one individually.
                    for (int j = 0; j < CharsPerUlong; j++)
                    {
                        char charValue = (char)value;
                        value >>= Bits.InChar;

                        if (charValue >= 255)
                            continue;

                        byte b = (byte)charValue;
                        count += EscapePositionsCountTable[b];
                        controlCount += EscapePositionsControlTable[b];
                    }
                }
            }

            // Handle remaining characters
            // Handle any remaining characters
            int processedChars = fullSegmentsBytes / 2;  // Convert bytes to char count
            for (int i = processedChars; i < _source.Length; i++)
            {
                char charValue = _source[i];
                if (charValue >= 255)
                    continue;

                byte b = (byte)charValue;
                count += EscapePositionsCountTable[b];
                controlCount += EscapePositionsControlTable[b];
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }


        [Benchmark]
        public int SuperScalarBranchlessAccumulators2()
        {
            const int CharsPerUlong = sizeof(ulong) / sizeof(char);  // Number of chars that fit in a word (8 bytes / 2 bytes per char)

            // Bytes covered by full 8-byte segments
            int fullSegmentsBytes = (_source.Length / CharsPerUlong) * sizeof(ulong);

            // Get a reference to the string as bytes
            ref byte strBytes = ref Unsafe.As<char, byte>(ref MemoryMarshal.GetReference<char>(_source));

            // Process full 8-byte segments using byte offsets
            int count0 = 0;
            int count1 = 0;
            int controlCount0 = 0;
            int controlCount1 = 0;
            for (nint byteOffset = 0; byteOffset < fullSegmentsBytes; byteOffset += sizeof(ulong))
            {
                // Read 8 bytes at the current offset
                ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref strBytes, byteOffset));

                // Character 0: bits 0-15
                int maskByte0 = -((byte)((value >> 8) & 0xFF) == 0 ? 1 : 0); // -1 if high_byte_0 is 0, 0 otherwise
                count0 += EscapePositionsCountTable[(byte)value] & maskByte0;
                controlCount0 += EscapePositionsControlTable[(byte)value] & maskByte0;

                // Character 1: bits 16-31
                int maskByte1 = -((byte)((value >> 24) & 0xFF) == 0 ? 1 : 0);
                count1 += EscapePositionsCountTable[(byte)(value >> 16)] & maskByte1;
                controlCount1 += EscapePositionsControlTable[(byte)(value >> 16)] & maskByte1;

                // Character 2: bits 32-47
                int maskByte2 = -((byte)((value >> 40) & 0xFF) == 0 ? 1 : 0);
                count0 += EscapePositionsCountTable[(byte)(value >> 32)] & maskByte2;
                controlCount0 += EscapePositionsControlTable[(byte)(value >> 32)] & maskByte2;

                // Character 3: bits 48-63
                int maskByte3 = -((byte)((value >> 56) & 0xFF) == 0 ? 1 : 0);
                count1 += EscapePositionsCountTable[(byte)(value >> 48)] & maskByte3;
                controlCount1 += EscapePositionsControlTable[(byte)(value >> 48)] & maskByte3;
            }

            int count = count0 + count1;
            int controlCount = controlCount0 + controlCount1;

            // Handle any remaining characters
            int processedChars = fullSegmentsBytes / 2;  // Convert bytes to char count
            for (int i = processedChars; i < _source.Length; i++)
            {
                char charValue = _source[i];
                if (charValue >= 255)
                    continue;

                byte b = (byte)charValue;
                count += EscapePositionsCountTable[b];
                controlCount += EscapePositionsControlTable[b];
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }

        [Benchmark]
        public int SuperScalarBranchless()
        {
            var count = 0;
            var controlCount = 0;

            const int CharsPerUlong = sizeof(ulong) / sizeof(char);  // Number of chars that fit in a word (8 bytes / 2 bytes per char)

            // Bytes covered by full 8-byte segments
            int fullSegmentsBytes = (_source.Length / CharsPerUlong) * sizeof(ulong);

            // Get a reference to the string as bytes
            ref byte strBytes = ref Unsafe.As<char, byte>(ref MemoryMarshal.GetReference<char>(_source));

            // Process full 8-byte segments using byte offsets
            for (nint byteOffset = 0; byteOffset < fullSegmentsBytes; byteOffset += sizeof(ulong))
            {
                // Read 8 bytes at the current offset
                ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref strBytes, byteOffset));

                // Character 0: bits 0-15
                int maskByte0 = -((byte)((value >> 8) & 0xFF) == 0 ? 1 : 0); // -1 if high_byte_0 is 0, 0 otherwise
                count += EscapePositionsCountTable[(byte)value] & maskByte0;
                controlCount += EscapePositionsControlTable[(byte)value] & maskByte0;

                // Character 1: bits 16-31
                int maskByte1 = -((byte)((value >> 24) & 0xFF) == 0 ? 1 : 0);
                count += EscapePositionsCountTable[(byte)(value >> 16)] & maskByte1;
                controlCount += EscapePositionsControlTable[(byte)(value >> 16)] & maskByte1;

                // Character 2: bits 32-47
                int maskByte2 = -((byte)((value >> 40) & 0xFF) == 0 ? 1 : 0);
                count += EscapePositionsCountTable[(byte)(value >> 32)] & maskByte2;
                controlCount += EscapePositionsControlTable[(byte)(value >> 32)] & maskByte2;

                // Character 3: bits 48-63
                int maskByte3 = -((byte)((value >> 56) & 0xFF) == 0 ? 1 : 0);
                count += EscapePositionsCountTable[(byte)(value >> 48)] & maskByte3;
                controlCount += EscapePositionsControlTable[(byte)(value >> 48)] & maskByte3;
            }

            // Handle any remaining characters
            int processedChars = fullSegmentsBytes / 2;  // Convert bytes to char count
            for (int i = processedChars; i < _source.Length; i++)
            {
                char charValue = _source[i];
                if (charValue >= 255)
                    continue;

                byte b = (byte)charValue;
                count += EscapePositionsCountTable[b];
                controlCount += EscapePositionsControlTable[b];
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }

        [Benchmark]
        public int SuperScalarBranchlessRef()
        {
            var count = 0;
            var controlCount = 0;

            const int CharsPerUlong = sizeof(ulong) / sizeof(char);  // Number of chars that fit in a word (8 bytes / 2 bytes per char)

            // Bytes covered by full 8-byte segments
            int length = _source.Length;
            int fullSegmentsBytes = (length / CharsPerUlong) * sizeof(ulong);

            // Get a reference to the string as bytes
            ref byte strBytes = ref Unsafe.As<char, byte>(ref MemoryMarshal.GetReference<char>(_source));
            ref int countTable = ref MemoryMarshal.GetReference<int>(EscapePositionsCountTable);
            ref int controlTable = ref MemoryMarshal.GetReference<int>(EscapePositionsControlTable);

            // Process full 8-byte segments using byte offsets
            for (nint byteOffset = 0; byteOffset < fullSegmentsBytes; byteOffset += sizeof(ulong))
            {
                // Read 8 bytes at the current offset
                ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref strBytes, byteOffset));
                
                // Character 0: bits 0-15
                int maskByte0 = -((byte)((value >> 8) & 0xFF) == 0 ? 1 : 0); // -1 if high_byte_0 is 0, 0 otherwise
                count += Unsafe.Add(ref countTable, (byte)value) & maskByte0;
                controlCount += Unsafe.Add(ref controlTable, (byte)value) & maskByte0;

                // Character 1: bits 16-31
                int maskByte1 = -((byte)((value >> 24) & 0xFF) == 0 ? 1 : 0);
                count += Unsafe.Add(ref countTable, (byte)(value>> 16)) & maskByte1;
                controlCount += Unsafe.Add(ref controlTable, (byte)value >> 16) & maskByte1;

                // Character 2: bits 32-47
                int maskByte2 = -((byte)((value >> 40) & 0xFF) == 0 ? 1 : 0);
                count += Unsafe.Add(ref countTable, (byte)(value >> 32)) & maskByte2;
                controlCount += Unsafe.Add(ref controlTable, (byte)(value >> 32)) & maskByte2;

                // Character 3: bits 48-63
                int maskByte3 = -((byte)((value >> 56) & 0xFF) == 0 ? 1 : 0);
                count += Unsafe.Add(ref countTable, (byte)(value >> 48)) & maskByte3;
                controlCount += Unsafe.Add(ref controlTable, (byte)(value >> 48)) & maskByte3;
            }

            // Handle any remaining characters
            int processedChars = fullSegmentsBytes;  // Convert bytes to char count
            for (int i = processedChars; i < length; i += sizeof(char))
            {
                char charValue = Unsafe.ReadUnaligned<char>(ref Unsafe.Add(ref strBytes, i));
                if (charValue >= 255)
                    continue;

                byte b = (byte)charValue;
                count += Unsafe.Add(ref countTable, (byte)b);
                controlCount += Unsafe.Add(ref controlTable, (byte)b);
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5 + controlCount * 5;
        }
    }
}
