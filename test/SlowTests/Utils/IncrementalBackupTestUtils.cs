// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupTestUtils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using SlowTests.Voron;

namespace SlowTests.Utils
{
    public class IncrementalBackupTestUtils : IDisposable
    {
        public IncrementalBackupTestUtils()
        {
            Clean();
        }

        public string IncrementalBackupFile(int n) =>
            Path.Combine(_dataDir, string.Format("voron-test.{0}-incremental-backup.zip", n));

        private string RestoredStoragePath => Path.Combine(_dataDir, "incremental-backup-test.data");

        private readonly string _dataDir = StorageTest.GenerateTempDirectoryWithoutCollisions();

        private void Clean()
        {
            foreach (var incBackupFile in Directory.EnumerateFiles(_dataDir, "*incremental-backup.zip"))
            {
                File.Delete(incBackupFile);
            }

            if (Directory.Exists(RestoredStoragePath))
                Directory.Delete(RestoredStoragePath, true);
        }

        public void Dispose()
        {
            Clean();
        }
    }
}
