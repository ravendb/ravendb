using System.Threading;

namespace Raven.Server.Commercial
{
    public class InterlockedLock
    {
        private int _lockStatus = 0;
        private const int Locked = 1;
        private const int UnLocked = 0;

        public bool TryEnter()
        {
            if (Interlocked.CompareExchange(ref _lockStatus, Locked, UnLocked) == Locked)
                return false;

            return true;
        }

        public void Exit()
        {
            Interlocked.Exchange(ref _lockStatus, UnLocked);
        }
    }
}