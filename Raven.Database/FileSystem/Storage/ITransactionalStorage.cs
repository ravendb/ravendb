using System;
using System.Diagnostics;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.FileSystem.Plugins;

namespace Raven.Database.FileSystem.Storage
{
    public interface ITransactionalStorage : IDisposable
    {
        Guid Id { get; }

		void Initialize(OrderedPartCollection<AbstractFileCodec> fileCodecs);

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        void Batch(Action<IStorageActionsAccessor> action);

        string FriendlyName { get; }

        void StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup, FileSystemDocument fileSystemDocument);
        void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output);

        void Compact(InMemoryRavenConfiguration configuration, Action<string> output);
    }
}