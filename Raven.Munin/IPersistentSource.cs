using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
    public interface IPersistentSource : IDisposable
    {
        T Read<T>(Func<Stream,T> readOnlyAction);

        IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction);

        void Write(Action<Stream> readWriteAction);

        bool CreatedNew { get; }

        void ReplaceAtomically(Stream log);

        Stream CreateTemporaryStream();

        void FlushLog();
        RemoteManagedStorageState CreateRemoteAppDomainState();
    }
}