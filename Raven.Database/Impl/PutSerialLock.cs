// -----------------------------------------------------------------------
//  <copyright file="PutSerialLock.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Impl
{
    using System;
    using System.Threading;

    using Raven.Abstractions.Extensions;

    public class PutSerialLock
    {
        private readonly object locker = new object();

        public IDisposable Lock()
        {
            Monitor.Enter(locker);
            return new DisposableAction(() => Monitor.Exit(locker));
        }

        public IDisposable TryLock(int timeoutInMilliseconds)
        {
            if (Monitor.TryEnter(locker, timeoutInMilliseconds))
                return new DisposableAction(() => Monitor.Exit(locker));

            return null;
        }
    }
}