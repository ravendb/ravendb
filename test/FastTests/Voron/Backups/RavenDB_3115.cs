// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3115.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Xunit.Abstractions;

namespace FastTests.Voron.Backups
{
    public class RavenDB_3115 : StorageTest
    {
        public RavenDB_3115(ITestOutputHelper output) : base(output)
        {
        }

        private readonly IncrementalBackupTestUtils _incrementalBackupTestUtils = new IncrementalBackupTestUtils();

        protected StorageEnvironmentOptions ModifyOptions(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.IncrementalBackupEnabled = true;
            options.ManualFlushing = true;

            return options;
        }

        [Fact]
        public void ShouldCorrectlyLoadAfterRestartIfIncrementalBackupWasDone()
        {
            var bytes = new byte[1024];

            new Random().NextBytes(bytes);

            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPathForTests(DataDir))))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree(  "items");

                    tx.Commit();
                }

                for (int j = 0; j < 100; j++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.ReadTree("items");

                        for (int i = 0; i < 100; i++)
                        {
                            tree.Add("items/" + i, bytes);
                        }

                        tx.Commit();
                    }
                }

                BackupMethods.Incremental.ToFile(env, _incrementalBackupTestUtils.IncrementalBackupFile(0));
            }

            // restart
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPathForTests(DataDir))))
            {
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            _incrementalBackupTestUtils.Dispose();
        }
    }
}
