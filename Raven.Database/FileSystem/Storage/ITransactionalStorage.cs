using System;
using System.Diagnostics;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage
{
    public interface ITransactionalStorage : IDisposable
    {
        Guid Id { get; }

        void Initialize();

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        void Batch(Action<IStorageActionsAccessor> action);

        string FriendlyName { get; }

        void StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup, FileSystemDocument fileSystemDocument);
        void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output);

        void Compact(InMemoryRavenConfiguration configuration);
    }
}