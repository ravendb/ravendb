// -----------------------------------------------------------------------
//  <copyright file="HugeTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using SlowTests.Utils;
using Voron;
using Voron.Util.Conversion;
using Xunit;
using Sparrow.Compression;
using Sparrow;

namespace SlowTests.Voron
{
    public class HugeTransactions : StorageTest
    {
        public const long Gb = 1024L*1024*1024;
        public const long HalfGb = 512L*1024*1024;
        public const long Mb = 1024L*1024;

        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
        public static Random Rand = new Random(123);

        [Theory]
        [InlineData(2)]
        [InlineData(6)]
        public unsafe void CanWriteBigTransactions(long transactionSizeInGb)
        {
            var tmpFile = $"{Path.GetTempPath()}{Path.DirectorySeparatorChar}TestBigTx" + transactionSizeInGb;
            try
            {
                Directory.Delete(tmpFile, true);
            }
            catch (Exception)
            {
                // ignored
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(tmpFile)))
            {
                var value = new byte[HalfGb];
                new Random().NextBytes(value);
                value[0] = 11;
                value[HalfGb - 1] = 22;
                value[(HalfGb/3)*2] = 33;
                value[HalfGb/2] = 44;
                value[HalfGb/3] = 55;

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("bigTree");

                    for (int i = 0; i < transactionSizeInGb*2; i++)
                    {
                        var ms1 = new MemoryStream(value);
                        ms1.Position = 0;
                        tree.Add("bigTreeKey" + i, ms1);
                    }

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
                    fixed (byte* singleByte = new byte[1])
                    {

                        for (int i = 0; i < transactionSizeInGb*2; i++)
                        {
                            var key = "bigTreeKey" + i;
                            var reader = tree.Read(key).Reader;

                            VerifyData(singleByte, reader, 0, 11);
                            VerifyData(singleByte, reader, (int) HalfGb - 1, 22);
                            VerifyData(singleByte, reader, ((int) HalfGb/3)*2, 33);
                            VerifyData(singleByte, reader, (int) HalfGb/2, 44);
                            VerifyData(singleByte, reader, (int) HalfGb/3, 55);
                        }
                    }
                }
            }
            Directory.Delete(tmpFile, true);
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

        [Fact]
        public unsafe void LZ4TestAbove2GB()
        {
            var options = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "bigLz4-test.data"));
            using (var env = new StorageEnvironment(options))
            {
                long Gb = 1024*1024*1024;
                long inputSize = 3L*Gb;
                byte* outputBuffer, inputBuffer, checkedBuffer;
                var outputBufferSize = CreateScratchFile("output", env, inputSize, out outputBuffer);
                var inputBufferSize = CreateScratchFile("input", env, inputSize, out inputBuffer);
                var checkedBufferSize = CreateScratchFile("checked", env, inputSize, out checkedBuffer);

                // write some data in known places in inputBuffer
                long compressedLen = 0;
                byte testNum = 0;
                for (long testPoints = 0; testPoints < inputSize; testPoints += Gb)
                {
                    var testPointer = (byte*) ((long) inputBuffer + testPoints);
                    *testPointer = ++testNum;
                }

                // encode inputBuffer into outputBuffer
                var lz4 = new LZ4();
                compressedLen = lz4.Encode64LongBuffer(inputBuffer, outputBuffer, inputSize, outputBufferSize);

                // decode outputBuffer into checkedBuffer
                var totalOutputSize = LZ4.Decode64LongBuffers(outputBuffer, compressedLen, checkedBuffer, inputSize, true);

                Assert.Equal(inputSize, totalOutputSize);

                testNum = 0;
                for (long testPoints = 0; testPoints < inputSize; testPoints += Gb)
                {
                    var testPointer = (byte*) ((long) checkedBuffer + testPoints);
                    Assert.Equal(testNum++, *testPointer);
                }
            }
        }

        private static unsafe long CreateScratchFile(string scratchName, StorageEnvironment env, long inputSize, out byte* buffer)
        {
            var filename = $"{Path.GetTempPath()}{Path.DirectorySeparatorChar}TestBigCompression-{scratchName}";
            var pager = env.Options.CreateScratchPager(filename);
            long bufferSize = LZ4.MaximumOutputLength(inputSize);
            int bufferSizeInPages = checked((int) (bufferSize/env.Options.PageSize));
            pager.EnsureContinuous(0, bufferSizeInPages);
            buffer = pager.AcquirePagePointer(null, 0);
            return bufferSize;
        }
    }
}

