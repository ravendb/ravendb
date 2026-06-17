using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public interface IReplicationManager : IDisposable
    {
        public IAsyncDisposable Break();
        public void Mend();
        public void ReplicateOnce(string docId);
        public Task EnsureNoReplicationLoopAsync();
    }
}
