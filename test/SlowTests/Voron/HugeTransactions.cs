// -----------------------------------------------------------------------
//  <copyright file="HugeTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using SlowTests.Utils;
using Voron;
using Voron.Util.Conversion;
using Xunit;

namespace SlowTests.Voron
{
    public class HugeTransactions : StorageTest
    {
        public const long Gb = 1024L * 1024 * 1024;
        public const long HalfGb = 512L * 1024 * 1024;
        public const long Mb = 1024L * 1024;

        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
        public static Random Rand = new Random(123);

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(6)]
        [InlineData(9)]
        public unsafe void CanWriteBigTransactions(long transactionSizeInGb)
        {
            var tmpFile = $"{Path.GetTempPath()}{Path.DirectorySeparatorChar}TestBigTx" + transactionSizeInGb; // TODO :: what happens in parallel ?
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
    }
}

