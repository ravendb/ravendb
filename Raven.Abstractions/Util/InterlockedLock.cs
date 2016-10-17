//-----------------------------------------------------------------------
// <copyright file="InterlockedLock.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;

namespace Raven.Abstractions.Util
{
    public class InterlockedLock
    {
        private int lockStatus = 0;
        private const int Locked = 1;
        private const int UnLocked = 0;

        public bool TryEnter()
        {
            if (Interlocked.CompareExchange(ref lockStatus, Locked, UnLocked) == Locked)
                return false;

            return true;
        }

        public void Exit()
        {
            Interlocked.Exchange(ref lockStatus, UnLocked);
        }
    }
}
