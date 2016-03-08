//-----------------------------------------------------------------------
// <copyright file="EsentBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Backup;

namespace Raven.Storage.Esent.Backup
{
    public class EsentBackup
    {
        private readonly JET_INSTANCE instance;
        private readonly string destination;
        private readonly BackupGrbit backupOptions;
        private readonly CancellationToken token;
        public event Action<string, Exception, BackupStatus.BackupMessageSeverity> Notify = delegate { };

        public EsentBackup(JET_INSTANCE instance, string destination, BackupGrbit backupOptions, CancellationToken token)
        {
            this.instance = instance;
            this.destination = destination;
            this.backupOptions = backupOptions;
            this.token = token;
        }

        public void Execute()
        {
            // TODO work out if we can get a % done from this, at the moment in only seems to give "Begin" and "End" messages
            var task = Task.Factory.StartNew(() => Api.JetBackupInstance(instance, destination, backupOptions, StatusCallback), TaskCreationOptions.LongRunning);

            while (!task.IsCompleted)
            {
                if (token.IsCancellationRequested)
                {
                    Api.JetStopBackupInstance(instance);
                }

                Thread.Sleep(250);
            }

            if (task.Exception != null)
            {
                var ex = task.Exception.ExtractSingleInnerException();
                if (ex is EsentBackupAbortByServerException)
                {
                    throw new OperationCanceledException(ex.Message);
                }
                throw ex;
            }
        }

        private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
        {
            Notify(string.Format("Esent {0} {1} {2}", snp, snt, data).Trim(), null, BackupStatus.BackupMessageSeverity.Informational);
            return JET_err.Success;
        }
    }
}
