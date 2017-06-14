using System;
using Microsoft.Xunit.Performance;
using Sparrow.Compression;

namespace Regression.Benchmark
{
    public unsafe class CompressionBench : BenchBase
    {
        private byte[] input = new byte[65 * 1024 * 1024];

        public CompressionBench()
        {
        }

        [Benchmark]
        public void HighRepetition()
        {
            var main = new Random(1000);

            int i = 0;
            while ( i < input.Length )
            {
                int sequenceNumber = main.Next(20);
                int sequenceLength = Math.Min( main.Next(128), input.Length - i );

                var rnd = new Random(sequenceNumber);
                for ( int j = 0; j < sequenceLength; j++, i++)
                    input[i] = (byte)(rnd.Next() % 255);
            }

            var maximumOutputLength = LZ4.MaximumOutputLength(input.Length);
            byte[] encodedOutput = new byte[maximumOutputLength];

            ExecuteBenchmark(() => 
            {
                fixed (byte* inputPtr = input)
                fixed (byte* encodedOutputPtr = encodedOutput)
                fixed (byte* outputPtr = input)
                {
                    int compressedSize = LZ4.Encode64(inputPtr, encodedOutputPtr, input.Length, (int)maximumOutputLength);
                    int uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, input.Length, true);
                }
            });
        }

        [Benchmark]
        public void LowBitsRandom()
        {
            int threshold = 1 << 4;
            var rnd = new Random(1000);
            for (int i = 0; i < input.Length; i++)
                input[i] = (byte)(rnd.Next() % threshold);

            var maximumOutputLength = LZ4.MaximumOutputLength(input.Length);
            byte[] encodedOutput = new byte[maximumOutputLength];

            ExecuteBenchmark(() => 
            {
                fixed (byte* inputPtr = input)
                fixed (byte* encodedOutputPtr = encodedOutput)
                fixed (byte* outputPtr = input)
                {
                    int compressedSize = LZ4.Encode64(inputPtr, encodedOutputPtr, input.Length, (int)maximumOutputLength);
                    int uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, input.Length, true);
                }
            });
        }
    }
}
