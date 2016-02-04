// -----------------------------------------------------------------------
//  <copyright file="FileSystemSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;

namespace Raven.Smuggler.FileSystem
{
    public class FileSystemSmuggler
    {
        private readonly FileSystemSmugglerOptions options;

        public FileSystemSmuggler(FileSystemSmugglerOptions options)
        {
            this.options = options;
            Notifications = new FileSystemSmugglerNotifications();
        }

        public FileSystemSmugglerNotifications Notifications { get; }

        public FileSystemSmugglerOperationState Execute(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination)
        {
            return AsyncHelpers.RunSync(() => ExecuteAsync(source, destination, CancellationToken.None));
        }

        public async Task<FileSystemSmugglerOperationState> ExecuteAsync(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (source) // TODO arek
            using (destination)
            {
                FileSystemSmugglerOperationState state = null;

                try
                {
                    await source
                        .InitializeAsync(options, cancellationToken)
                        .ConfigureAwait(false);

                    await destination
                        .InitializeAsync(options, Notifications, cancellationToken)
                        .ConfigureAwait(false);

                    state = new FileSystemSmugglerOperationState
                                {
                                    LastFileEtag = options.StartFilesEtag,
                                    LastDeletedFileEtag = options.StartFilesDeletionEtag,
                                };

                    Debug.Assert(state.LastFileEtag != null); //TODO arek - for refactoring purposes
                    Debug.Assert(state.LastDeletedFileEtag != null);

                    await ProcessAsync(source, destination, state, cancellationToken).ConfigureAwait(false);
                    
                    await destination.AfterExecuteAsync(state).ConfigureAwait(false);

                    return state;
                }
                catch (SmugglerException e)
                {
                    // TODO arek
                    //source.OnException(e);
                    destination.OnException(e);

                    throw;
                }
            }
        }

        private async Task ProcessAsync(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination, FileSystemSmugglerOperationState state, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(source.DisplayName) == false)
                Notifications.ShowProgress("Processing source: {0}", source.DisplayName);

            var maxEtags = await source
                        .FetchCurrentMaxEtagsAsync()
                        .ConfigureAwait(false);

            foreach (var type in source.GetItemsToSmuggle())
            {
                switch (type)
                {
                    case SmuggleType.None:
                        return;
                    case SmuggleType.File:
                        await new FileSmuggler(source, destination, options, Notifications, maxEtags.LastFileEtag).SmuggleAsync(state, cancellationToken).ConfigureAwait(false);
                        continue;
                    case SmuggleType.Configuration:
                        await new ConfigurationSmuggler(source, destination, options, Notifications).SmuggleAsync(state, cancellationToken).ConfigureAwait(false);
                        continue;
                    default:
                        throw new NotSupportedException(type.ToString());
                }
            }
        }
    }
}