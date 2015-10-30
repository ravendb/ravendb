// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3742.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Backup;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3742 : RavenFilesTestBase
    {
        private readonly string BackupDir;

        public RavenDB_3742()
        {
            BackupDir = NewDataPath("RavenDB_3742_Backup");
        }

        [Theory]
        [PropertyData("Storages")]
        public void file_system_backup_operation_sucessfully_waits_for_backup_to_complete(string requestedStorage)
        {
            using (var store = NewStore(runInMemory: false, requestedStorage: "esent"))
            {
                using (var operation = new FilesystemBackupOperation(new BackupParameters
                {
                    BackupPath = BackupDir,
                    ServerUrl = store.Url,
                    Filesystem = store.DefaultFileSystem
                }))
                {

                    Assert.True(operation.InitBackup());
                    Assert.DoesNotThrow(() => operation.WaitForBackup());
                }
            }
        }
    }
}
