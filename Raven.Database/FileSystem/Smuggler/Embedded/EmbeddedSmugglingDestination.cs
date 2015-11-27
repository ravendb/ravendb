// -----------------------------------------------------------------------
//  <copyright file="EmbeddedFilesSmugglingDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Util;
using Raven.Smuggler.FileSystem;

namespace Raven.Database.FileSystem.Smuggler.Embedded
{
    public class EmbeddedSmugglingDestination : IFileSystemSmugglerDestination
    {
        private readonly RavenFileSystem fileSystem;

        public EmbeddedSmugglingDestination(RavenFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        public void Dispose()
        {
        }

        public Task InitializeAsync(FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public ISmuggleFilesToDestination WriteFiles()
        {
            return new SmuggleFilesToEmbedded(fileSystem);
        }

        public ISmuggleConfigurationsToDestination WriteConfigurations()
        {
            return new SmuggleConfigurationsToEmbedded(fileSystem);
        }

        public Task AfterExecuteAsync(FileSystemSmugglerOperationState state)
        {
            return new CompletedTask();
        }
    }
}