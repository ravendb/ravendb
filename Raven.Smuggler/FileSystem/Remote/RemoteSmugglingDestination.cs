// -----------------------------------------------------------------------
//  <copyright file="RemoteSmugglingDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;
using Raven.Smuggler.Helpers;

namespace Raven.Smuggler.FileSystem.Remote
{
    public class RemoteSmugglingDestination : IFileSystemSmugglerDestination
    {
        private readonly FilesConnectionStringOptions connectionOptions;

        private FilesStore filesStore;

        public RemoteSmugglingDestination(FilesConnectionStringOptions connectionOptions)
        {
            this.connectionOptions = connectionOptions;
        }

        public async Task InitializeAsync(FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, CancellationToken cancellationToken)
        {
            filesStore = FileStoreHelper.CreateStore(connectionOptions);

            await ServerValidation.ValidateThatServerIsUpAndFileSystemExists(connectionOptions, filesStore).ConfigureAwait(false);

            await ServerValidation.DetectServerSupportedFeatures(connectionOptions).ConfigureAwait(false); // TODO arek - merge those 2 methods into single one
        }

        public ISmuggleFilesToDestination WriteFiles()
        {
            return new SmuggleFilesToRemote(filesStore);
        }

        public ISmuggleConfigurationsToDestination WriteConfigurations()
        {
            return new SmuggleConfigurationsToRemote(filesStore);
        }

        public Task AfterExecuteAsync(FileSystemSmugglerOperationState state)
        {
            return new CompletedTask();
        }

        public void OnException(SmugglerException exception)
        {
        }

        public void Dispose()
        {
            filesStore?.Dispose();
        }
    }
}