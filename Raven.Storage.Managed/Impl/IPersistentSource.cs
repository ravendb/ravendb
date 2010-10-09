using System;
using System.IO;

namespace Raven.Storage.Managed.Impl
{
    public interface IPersistentSource : IDisposable
    {
        object SyncLock { get; }
        Stream Data { get; }
        Stream Log { get; }
        bool CreatedNew { get; }

        void ReplaceAtomically(Stream data, Stream log);

        Stream CreateTemporaryStream();

        void FlushData();
        void FlushLog();
    }
}