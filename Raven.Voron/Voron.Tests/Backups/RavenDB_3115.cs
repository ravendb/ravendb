// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3115.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
    public class RavenDB_3115 : IDisposable
    {
        protected StorageEnvironmentOptions ModifyOptions(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
            options.IncrementalBackupEnabled = true;
            options.ManualFlushing = true;

            return options;
        }

        public RavenDB_3115()
        {
            Clean();
        }

        [PrefixesFact]
        public void ShouldCorrectlyLoadAfterRestartIfIncrementalBackupWasDone()
        {
            var bytes = new byte[1024];

            new Random().NextBytes(bytes);

            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath("Data"))))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "items");

                    tx.Commit();
                }

                for (int j = 0; j < 100; j++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = tx.ReadTree("items");

                        for (int i = 0; i < 100; i++)
                        {
                            tree.Add("items/" + i, bytes);
                        }

                        tx.Commit();
                    }
                }

                BackupMethods.Incremental.ToFile(env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);
            }

            // restart
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath("Data"))))
            {
            }
        }

        public void Dispose()
        {
            Clean();
        }

        private static void Clean()
        {
            if (Directory.Exists("Data"))
                Directory.Delete("Data", true);

            IncrementalBackupTestUtils.Clean();
        }
    }
}
