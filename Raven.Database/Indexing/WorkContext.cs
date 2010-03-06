using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Database.Data;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    public class WorkContext
    {
        private volatile bool doWork = true;
        private readonly object waitForWork = new object();

        private readonly ConcurrentQueue<ServerError> serverErrors = new ConcurrentQueue<ServerError>();

        public bool DoWork
        {
            get
            {
                return doWork;
            }
        }

        public IndexStorage IndexStorage { get; set; }

        public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

        public TransactionalStorage TransactionaStorage { get; set; }

        public void WaitForWork()
        {
            if (!doWork) 
                return;
            lock (waitForWork)
            {
                Monitor.Wait(waitForWork);
            }
        }

        public void NotifyAboutWork()
        {
            lock (waitForWork)
            {
                Monitor.PulseAll(waitForWork);
            }
        }

        public void StopWork()
        {
            doWork = false;
            lock (waitForWork)
            {
                Monitor.PulseAll(waitForWork);
            }
        }

        public void AddError(string index, string key, string error)
        {
            serverErrors.Enqueue(new ServerError
            {
                Document = key,
                Error = error,
                Index = index,
                Timestamp = DateTime.Now
            });
            if (serverErrors.Count <= 50) 
                return;
            ServerError ignored;
            serverErrors.TryDequeue(out ignored);
        }

        public ServerError[] Errors
        {
            get
            {
                return serverErrors.ToArray();
            }
        }
    }
}