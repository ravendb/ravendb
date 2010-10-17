using System;
using System.IO;

namespace Raven.Storage.Managed.Impl
{
    public interface IPersistentSource : IDisposable
    {
        object SyncLock { get; }
        Stream Log { get; }
        bool CreatedNew { get; }

        void ReplaceAtomically(Stream log);

        Stream CreateTemporaryStream();

        void FlushLog();
        RemoteManagedStorageState CreateRemoteAppDomainState();
    }
}