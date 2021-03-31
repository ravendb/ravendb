// -----------------------------------------------------------------------
//  <copyright file="PutSerialLock.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lucene.Net.Store;
using Raven.Database.Config;

namespace Raven.Database.Impl
{
    using System;
    using System.Threading;

    using Raven.Abstractions.Extensions;

    public class PutSerialLock
    {
        private readonly object locker = new object();
        private readonly TimeSpan lockDuration;

        public PutSerialLock(InMemoryRavenConfiguration configuration)
        {
            lockDuration = configuration.Storage.PutSerialLockDuration;
        }

        public IDisposable Lock()
        {
            if (Monitor.TryEnter(locker, lockDuration))
                return new DisposableAction(() => Monitor.Exit(locker));
            throw new SynchronizationLockException("Could not get the PUT serial lock in (un)reasonable timeframe (" + lockDuration + "), aborting operation");
        }

        public IDisposable TryLock(int timeoutInMilliseconds)
        {
            if (Monitor.TryEnter(locker, timeoutInMilliseconds))
                return new DisposableAction(() => Monitor.Exit(locker));

            return null;
        }
    }
}
