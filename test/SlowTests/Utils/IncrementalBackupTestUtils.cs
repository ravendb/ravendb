// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupTestUtils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using SlowTests.Voron;

namespace SlowTests.Utils
{
    public class IncrementalBackupTestUtils
    {
        public string IncrementalBackupFile(int n) =>
            Path.Combine(DataDir, string.Format("voron-test.{0}-incremental-backup.zip", n));

        public string RestoredStoragePath => Path.Combine(DataDir, "incremental-backup-test.data");

        public string DataDir = StorageTest.GenerateDataDir();

        public void Clean()
        {
            foreach (var incBackupFile in Directory.EnumerateFiles(DataDir, "*incremental-backup.zip"))
            {
                File.Delete(incBackupFile);
            }

            if (Directory.Exists(RestoredStoragePath))
                Directory.Delete(RestoredStoragePath, true);
        } 
    }
}
