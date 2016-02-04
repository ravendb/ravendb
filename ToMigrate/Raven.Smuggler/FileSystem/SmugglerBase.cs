// -----------------------------------------------------------------------
//  <copyright file="FileSystemSmugglerBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;

namespace Raven.Smuggler.FileSystem
{
    internal abstract class SmugglerBase
    {
        protected readonly FileSystemSmugglerNotifications Notifications;

        protected readonly FileSystemSmugglerOptions Options;

        protected readonly IFileSystemSmugglerSource Source;

        protected readonly IFileSystemSmugglerDestination Destination;

        protected SmugglerBase(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination, FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications)
        {
            Notifications = notifications;
            Source = source;
            Destination = destination;
            Options = options;
        }

        public abstract Task SmuggleAsync(FileSystemSmugglerOperationState state, CancellationToken cancellationToken);
    }
}