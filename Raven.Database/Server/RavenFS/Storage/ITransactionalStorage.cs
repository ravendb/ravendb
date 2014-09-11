using System;
using System.Diagnostics;

using Raven.Abstractions.Data;

namespace Raven.Database.Server.RavenFS.Storage
{
    public interface ITransactionalStorage : IDisposable
    {
        Guid Id { get; }

        void Initialize();

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        void Batch(Action<IStorageActionsAccessor> action);

        void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument documentDatabase);
        void Restore(RestoreRequest restoreRequest, Action<string> output);
    }
}