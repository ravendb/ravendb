using System;

namespace Raven.NewClient.Client.Changes
{
    public interface IConnectableChanges
    {
        bool Connected { get; }
        event EventHandler ConnectionStatusChanged;
        void WaitForAllPendingSubscriptions();
    }
}
