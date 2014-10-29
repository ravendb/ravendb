using System;
using System.Transactions;

namespace Raven.Tests.Common.Util
{
    public class DummyEnlistmentNotification : IEnlistmentNotification
    {
        public static readonly Guid Id = Guid.NewGuid();

        public bool WasCommitted { get; set; }
        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            WasCommitted = true;
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }
    }
}