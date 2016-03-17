// -----------------------------------------------------------------------
//  <copyright file="SynchronizationContext.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Logging;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationTaskContext : IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private volatile bool doWork = true;
        private readonly object waitForWork = new object();
        private long workCounter;

        public SynchronizationTaskContext()
        {
            LastSuccessfulSynchronizationTime = DateTime.MinValue;
        }

        public bool DoWork
        {
            get { return doWork; }
        }

        public CancellationToken CancellationToken
        {
            get { return cancellationTokenSource.Token; }
        }

        public DateTime LastSuccessfulSynchronizationTime { get; private set; }

        public void UpdateSuccessfulSynchronizationTime()
        {
            LastSuccessfulSynchronizationTime = SystemTime.UtcNow;
        }

        public bool WaitForWork(TimeSpan timeout, ref long workerWorkCounter)
        {
            if (!doWork)
                return false;
            var currentWorkCounter = Thread.VolatileRead(ref workCounter);
            if (currentWorkCounter != workerWorkCounter)
            {
                workerWorkCounter = currentWorkCounter;
                return true;
            }
            lock (waitForWork)
            {
                if (!doWork)
                    return false;
                currentWorkCounter = Thread.VolatileRead(ref workCounter);
                if (currentWorkCounter != workerWorkCounter)
                {
                    workerWorkCounter = currentWorkCounter;
                    return true;
                }

                Log.Debug("No work was found, workerWorkCounter: {0}, will wait for additional work", workerWorkCounter);
                var forWork = Monitor.Wait(waitForWork, timeout);

                return forWork;
            }
        }

        public void NotifyAboutWork()
        {
            lock (waitForWork)
            {
                if (doWork == false)
                {
                    return;
                }

                var increment = Interlocked.Increment(ref workCounter);
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Incremented work counter to {0}", increment);
                }

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

            cancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            cancellationTokenSource.Dispose();
        }
    }
}
