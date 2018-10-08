// -----------------------------------------------------------------------
//  <copyright file="HugeTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron;
using Sparrow.Compression;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;

namespace StressTests.Voron
{
    public class HugeTransactions : StorageTest
    {
        public const long Gb = 1024L * 1024 * 1024;
        public const long HalfGb = 512L * 1024 * 1024;
        public const long Mb = 1024L * 1024;

        public static Random Rand = new Random(123);

        [Theory64Bit]
        [InlineData(2)]
        [InlineData(6, Skip = "Too large to run on scratch machines. For manual run only")]
        public void CanWriteBigTransactions(int transactionSizeInGb)
        {
            var tmpFile = RavenTestHelper.NewDataPath(nameof(CanWriteBigTransactions), transactionSizeInGb, forceCreateDir: true);
            try
            {
                Directory.Delete(tmpFile, true);
            }
            catch (Exception)
            {
                // ignored
            }

            try
            {
                var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(tmpFile);
                storageEnvironmentOptions.ManualFlushing = true;
                using (var env = new StorageEnvironment(storageEnvironmentOptions))
                {
                    var value = new byte[HalfGb];
                    var random = new Random();
                    //var seed = random.Next();
                    //Console.WriteLine(seed);
                    new Random(240130173).NextBytes(value);
                    value[0] = 11;
                    value[HalfGb - 1] = 22;
                    value[(HalfGb / 3) * 2] = 33;
                    value[HalfGb / 2] = 44;
                    value[HalfGb / 3] = 55;

                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("bigTree");

                        for (int i = 0; i < transactionSizeInGb * 2; i++)
                        {
                            var ms1 = new MemoryStream(value);
                            ms1.Position = 0;
                            tree.Add("bigTreeKey" + i, ms1);
                        }
                        ValidateTree(transactionSizeInGb, tree);
                        tx.Commit();
                    }

                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("AddtionalTree");
                        var ms1 = new MemoryStream(value);
                        ms1.Position = 0;
                        tree.Add("treeKey1", ms1);

                        var ms2 = new MemoryStream(value);
                        ms2.Position = 0;
                        tree.Add("treeKey2", ms2);

                        tx.Commit();
                    }



                    using (var snapshot = env.ReadTransaction())
                    {
                        var tree = snapshot.ReadTree("bigTree");
                        ValidateTree(transactionSizeInGb, tree);
                    }
                }
            }
            finally
            {
                Directory.Delete(tmpFile, true);
            }
        }

        private static unsafe void ValidateTree(long transactionSizeInGb, Tree tree)
        {
            fixed (byte* singleByte = new byte[1])
            {
                for (int i = 0; i < transactionSizeInGb * 2; i++)
                {
                    var key = "bigTreeKey" + i;
                    var reader = tree.Read(key).Reader;

                    VerifyData(singleByte, reader, 0, 11);
                    VerifyData(singleByte, reader, (int)HalfGb - 1, 22);
                    VerifyData(singleByte, reader, ((int)HalfGb / 3) * 2, 33);
                    VerifyData(singleByte, reader, (int)HalfGb / 2, 44);
                    VerifyData(singleByte, reader, (int)HalfGb / 3, 55);
                }
            }
        }

        private static unsafe void VerifyData
            (byte* singleByte, ValueReader reader, int pos, int desired)
        {
            int val;
            reader.Skip(pos);
            reader.Read(singleByte, 1);
            val = *singleByte;
            Assert.Equal(desired, val);
        }

        [Theory64Bit]
        [InlineData(3L * 1024 * 1024 * 1024)] // in = 3GB, out ~= 4MB
        [InlineData(2)] // in = 3GB, out ~= 1.5GB
        [InlineData(1)] // in = 3GB, out > 3GB (rare case)
        [InlineData(0)] // special case : in = Exactly 1GB, out > 1GB
        public unsafe void LZ4TestAbove2GB(long divider)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10)))
            {
                var options = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, $"bigLz4-test-{divider}.data"));
                using (var env = new StorageEnvironment(options))
                {
                    long gb = 1024 * 1024 * 1024;
                    long inputSize = 3L * gb;
                    var guid = Guid.NewGuid();

                    using (var outputPager = CreateScratchFile($"output-{divider}-{guid}", env, inputSize, out byte* outputBuffer))
                    using (var inputPager = CreateScratchFile($"input-{divider}-{guid}", env, inputSize, out byte* inputBuffer))
                    using (var checkedPager = CreateScratchFile($"checked-{divider}-{guid}", env, inputSize, out byte* checkedBuffer))
                    {
                        var random = new Random(123);

                        if (divider != 0)
                        {
                            for (long p = 0; p < inputSize / divider; p++)
                            {
                                cts.Token.ThrowIfCancellationRequested();

                                (*(byte*)((long)inputBuffer + p)) = Convert.ToByte(random.Next(0, 255));
                            }
                        }
                        else
                        {
                            inputSize = int.MaxValue / 2 - 1; // MAX_INPUT_LENGTH_PER_SEGMENT
                            for (long p = 0; p < inputSize; p++)
                            {
                                cts.Token.ThrowIfCancellationRequested();

                                (*(byte*)((long)inputBuffer + p)) = Convert.ToByte(random.Next(0, 255));
                            }
                        }

                        Console.WriteLine("Calculating LZ4 MaximumOutputLength...");
                        var outputBufferSize = LZ4.MaximumOutputLength(inputSize);
                        Console.WriteLine("...done");

                        // write some data in known places in inputBuffer
                        byte testNum = 0;
                        for (long testPoints = 0; testPoints < inputSize; testPoints += gb)
                        {
                            cts.Token.ThrowIfCancellationRequested();

                            var testPointer = (byte*)((long)inputBuffer + testPoints);
                            *testPointer = ++testNum;
                        }

                        Console.WriteLine("Encoding LZ4 LongBuffer...");
                        // encode inputBuffer into outputBuffer
                        var compressedLenTask = Task.Factory.StartNew(() => LZ4.Encode64LongBuffer(inputBuffer, outputBuffer, inputSize, outputBufferSize), cts.Token);
                        compressedLenTask.Wait(cts.Token);
                        var compressedLen = compressedLenTask.Result;
                        Console.WriteLine("...done");

                        Console.WriteLine("Decoding LZ4 LongBuffers...");
                        // decode outputBuffer into checkedBuffer
                        var totalOutputSizeTask = Task.Factory.StartNew(() => LZ4.Decode64LongBuffers(outputBuffer, compressedLen, checkedBuffer, inputSize, true), cts.Token);
                        totalOutputSizeTask.Wait(cts.Token);
                        var totalOutputSize = totalOutputSizeTask.Result;
                        Console.WriteLine("...done");

                        Assert.Equal(compressedLen, totalOutputSize);

                        testNum = 0;
                        for (long testPoints = 0; testPoints < inputSize; testPoints += gb)
                        {
                            cts.Token.ThrowIfCancellationRequested();

                            var testPointer = (byte*)((long)checkedBuffer + testPoints);
                            Assert.Equal(++testNum, *testPointer);
                        }
                    }
                }
            }
        }

        private static unsafe AbstractPager CreateScratchFile(string scratchName, StorageEnvironment env, long inputSize, out byte* buffer)
        {
            Console.WriteLine($"Creating Scratch File: {scratchName}");

            var filename = Path.Combine(RavenTestHelper.NewDataPath(nameof(HugeTransactions), 0, forceCreateDir: true), $"TestBigCompression-{scratchName}");
            
            long bufferSize = LZ4.MaximumOutputLength(inputSize);
            int bufferSizeInPages = checked((int)(bufferSize / Constants.Storage.PageSize));

            Console.WriteLine($"CreateScratchPager. Size: {bufferSize}. Pages: {bufferSizeInPages}. Name: {scratchName}");
            var pager = env.Options.CreateScratchPager(filename, (long)bufferSizeInPages * Constants.Storage.PageSize);

            Console.WriteLine($"EnsureContinuous. Pages: {bufferSizeInPages}. Name: {scratchName}");
            pager.EnsureContinuous(0, bufferSizeInPages);

            Console.WriteLine($"AcquirePagePointer. Name: {scratchName}");
            buffer = pager.AcquirePagePointer(null, 0);

            return pager;
        }
    }
}

