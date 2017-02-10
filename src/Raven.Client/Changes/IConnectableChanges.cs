using System;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public interface IConnectableChanges
    {
        bool Connected { get; }
        event EventHandler ConnectionStatusChanged;
        void WaitForAllPendingSubscriptions();
    }

    public interface IConnectableChanges<T> : IConnectableChanges where T : IConnectableChanges
    {
        Task<T> ConnectionTask { get; }
    }
}
