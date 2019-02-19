// -----------------------------------------------------------------------
//  <copyright file="StorageCompactionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Utils;
using FastTests.Voron;
using Voron;
using Xunit;
using Voron.Data;
using Voron.Impl.Paging;

namespace SlowTests.Voron
{
    public class Checksum : StorageTest
    {
        [Fact]
        public unsafe void ValidatePageChecksumShouldDetectDataCorruption()
        {
            // Create some random data
            var treeNames = new List<string>();

            var random = new Random();

            var value1 = new byte[random.Next(1024 * 1024 * 2)];
            var value2 = new byte[random.Next(1024 * 1024 * 2)];

            random.NextBytes(value1);
            random.NextBytes(value2);

            const int treeCount = 5;
            const int recordCount = 6;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                env.Options.ManualFlushing = true;

                for (int i = 0; i < treeCount; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        string name = "tree/" + i;
                        treeNames.Add(name);

                        var tree = tx.CreateTree(name);

                        for (int j = 0; j < recordCount; j++)
                        {
                            tree.Add(string.Format("{0}/items/{1}", name, j), j % 2 == 0 ? value1 : value2);
                        }

                        tx.Commit();
                    }
                }
                env.FlushLogToDataFile();
            }

            // Lets corrupt something
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            using (var pager = LinuxTestUtils.GetNewPager(options, DataDir, "Raven.Voron"))
            using (var tempTX = new TempPagerTransaction())
            {
                var writePtr = pager.AcquirePagePointer(tempTX, 2) + PageHeader.SizeOf + 43; // just some random place on page #2
                for (byte i = 0; i < 8; i++)
                {
                    writePtr[i] = i;
                }
            }

            // Now lets try to read it all back and hope we get an exception
            try
            {
                using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
                {
                    using (var tx = env.ReadTransaction())
                    {

                        foreach (var treeName in treeNames)
                        {
                            var tree = tx.CreateTree(treeName);

                            for (int i = 0; i < recordCount; i++)
                            {
                                var readResult = tree.Read(string.Format("{0}/items/{1}", treeName, i));

                                Assert.NotNull(readResult);

                                if (i % 2 == 0)
                                {
                                    var readBytes = new byte[value1.Length];
                                    readResult.Reader.Read(readBytes, 0, readBytes.Length);
                                }
                                else
                                {
                                    var readBytes = new byte[value2.Length];
                                    readResult.Reader.Read(readBytes, 0, readBytes.Length);
                                }
                            }
                        }
                    }

                }
            }
            catch (Exception e)
            {
                Assert.True(e is InvalidOperationException || e is InvalidDataException);
            }
        }
    }
}
