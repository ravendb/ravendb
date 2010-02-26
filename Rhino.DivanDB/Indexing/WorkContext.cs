using System.Threading;

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