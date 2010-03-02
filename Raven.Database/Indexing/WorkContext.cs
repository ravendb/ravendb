using System;
using System.Threading;
using Rhino.DivanDB.Storage;

namespace Rhino.DivanDB.Indexing
{
    public class WorkContext
    {
        private volatile bool doWork = true;
        private readonly object waitForWork = new object();

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
    }
}