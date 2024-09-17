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
using Sparrow.Server.Platform;
using Voron;
using Xunit;
using Voron.Impl.Paging;
using Xunit.Abstractions;
using Voron.Global;

namespace SlowTests.Voron
{
    public class Checksum : StorageTest
    {
        public Checksum(ITestOutputHelper output) : base(output)
        {
        }

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
            using (var f = File.OpenWrite(Path.Combine(DataDir, "Raven.Voron")))
            {
                // just some random place on page #2
                f.Seek(2 * Constants.Storage.PageSize + PageHeader.SizeOf + 43, SeekOrigin.Begin);
                f.Write([0,1,2,3,4,5,6,7]);
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
