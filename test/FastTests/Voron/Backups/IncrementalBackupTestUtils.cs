// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupTestUtils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Server.Utils;

namespace FastTests.Voron.Backups
{
    public class IncrementalBackupTestUtils : IDisposable
    {
        public string IncrementalBackupFile(int n) => Path.Combine(_dataDir, string.Format("voron-test.{0}-incremental-backup.zip", n));

        public string RestoredStoragePath => Path.Combine(_dataDir, "incremental-backup-test.data");

        private readonly string _dataDir = RavenTestHelper.NewDataPath(nameof(IncrementalBackupTestUtils), 0, forceCreateDir: true);

        public void Dispose()
        {
            IOExtensions.DeleteDirectory(_dataDir);
            IOExtensions.DeleteDirectory(RestoredStoragePath);
        }
    }
}
