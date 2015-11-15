using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public interface IConnectableChanges<T> : IConnectableChanges where T : IConnectableChanges
    {
        Task<T> Task { get; }
    }
}
