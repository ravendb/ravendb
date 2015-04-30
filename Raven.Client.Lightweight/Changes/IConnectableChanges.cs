using System;

namespace Raven.Client.Changes
{
    public interface IConnectableChanges
    {
        bool Connected { get; }
        event EventHandler ConnectionStatusChanged;
        void WaitForAllPendingSubscriptions();
    }
}
