// -----------------------------------------------------------------------
//  <copyright file="BackupOperationDispatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Backup
{
    /// <summary>
    /// Class used to determinate is user wants to perform database backup or file system backup.
    /// </summary>
    public class BackupOperationDispatcher
    {
        public bool PerformBackup(BackupParameters parameters)
        {
            AbstractBackupOperation op;

            if (!string.IsNullOrWhiteSpace(parameters.Filesystem))
            {
                op = new FilesystemBackupOperation(parameters);
            }
            else
            {
                op = new DatabaseBackupOperation(parameters);
            }

            try
            {
                if (op.InitBackup())
                {
                    op.WaitForBackup();
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                op.Dispose();
            }
            return false;

        }
    }
}
