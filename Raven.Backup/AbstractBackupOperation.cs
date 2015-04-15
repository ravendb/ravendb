using System.Collections.Generic;
using System.Net.Http;

using Raven.Abstractions.Data;
using Raven.Client.Connection;

using System;
using System.Threading;
using Raven.Client.Connection.Implementation;

namespace Raven.Backup
{
    public abstract class AbstractBackupOperation : IDisposable
    {
        protected BackupParameters parameters;

        protected AbstractBackupOperation(BackupParameters parameters)
        {
            this.parameters = parameters;
        }

        public abstract bool InitBackup();

        protected abstract HttpJsonRequest CreateRequest(string url, HttpMethod method);

        public void WaitForBackup()
        {
            BackupStatus status = null;
            var messagesSeenSoFar = new HashSet<BackupStatus.BackupMessage>();

            while (status == null)
            {
                Thread.Sleep(100); // Allow the server to process the request
                status = GetStatusDoc();
            }

            if (parameters.NoWait)
            {
                Console.WriteLine("Backup operation has started, status is logged at Raven/Backup/Status");
                return;
            }

            while (status.IsRunning)
            {
                // Write out the messages as we poll for them, don't wait until the end, this allows "live" updates
                foreach (var msg in status.Messages)
                {
                    if (messagesSeenSoFar.Add(msg))
                    {
                        Console.WriteLine("[{0}] {1}", msg.Timestamp, msg.Message);
                    }
                }

                Thread.Sleep(1000);
                status = GetStatusDoc();
            }

            // After we've know it's finished, write out any remaining messages
            foreach (var msg in status.Messages)
            {
                if (messagesSeenSoFar.Add(msg))
                {
                    Console.WriteLine("[{0}] {1}", msg.Timestamp, msg.Message);
                }
            }
        }

        public abstract BackupStatus GetStatusDoc();

        public abstract void Dispose();
    }
}