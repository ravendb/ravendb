// -----------------------------------------------------------------------
//  <copyright file="IFileSystemSmugglerDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;

namespace Raven.Smuggler.FileSystem
{
    public interface IFileSystemSmugglerDestination : IDisposable
    {
        Task InitializeAsync(FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, CancellationToken cancellationToken);

        ISmuggleFilesToDestination WriteFiles();

        ISmuggleConfigurationsToDestination WriteConfigurations();

        Task AfterExecuteAsync(FileSystemSmugglerOperationState state);
    }
}