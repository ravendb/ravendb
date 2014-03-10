using System;
using System.Diagnostics;

namespace Raven.Database.Server.RavenFS.Storage
{
    public interface ITransactionalStorage : IDisposable
    {
        Guid Id { get; }

        bool Initialize();

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        void Batch(Action<IStorageActionsAccessor> action);
    }
}