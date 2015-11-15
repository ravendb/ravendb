using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public interface IConnectableChanges
    {
        bool Connected { get; }
        event EventHandler ConnectionStatusChanged;
        void WaitForAllPendingSubscriptions();
    }
}
