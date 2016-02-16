using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public interface IConnectableChanges<T> : IConnectableChanges where T : IConnectableChanges
    {
        Task<T> ConnectionTask { get; }
    }
}
