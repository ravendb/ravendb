// -----------------------------------------------------------------------
//  <copyright file="FileSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem
{
    internal class FileSmuggler : SmugglerBase
    {
        private readonly Etag maxEtag;

        public FileSmuggler(IFileSystemSmugglerSource source, IFileSystemSmugglerDestination destination, FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, Etag maxEtag)
            : base(source, destination, options, notifications)
        {
            this.maxEtag = maxEtag;
        }

        public override async Task SmuggleAsync(FileSystemSmugglerOperationState state, CancellationToken cancellationToken)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);

            Notifications.ShowProgress("Exporting Files");

            using (var writer = Destination.WriteFiles())
            {
                try
                {
                    while (true)
                    {
                        bool hasDocs = false;

                        using (var files = await Source.GetFilesAsync(state.LastFileEtag, Options.BatchSize, cancellationToken).ConfigureAwait(false))
                        {
                            while (await files.MoveNextAsync().ConfigureAwait(false))
                            {
                                hasDocs = true;

                                var file = files.Current;
                                if (file.IsTombstone)
                                {
                                    state.LastFileEtag = file.Etag;
                                    continue;
                                }

                                var tempLastEtag = file.Etag; // TODO arek - why do we need temp variable 
                                if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0) break;

                                if (Options.StripReplicationInformation)
                                    file.Metadata = StripReplicationInformationFromMetadata(file.Metadata);

                                if (Options.ShouldDisableVersioningBundle)
                                    file.Metadata = DisableVersioning(file.Metadata);

                                using (var fileStream = await Source.DownloadFileAsync(file).ConfigureAwait(false))
                                {
                                    await writer.WriteFileAsync(file, fileStream).ConfigureAwait(false);
                                }

                                state.LastFileEtag = tempLastEtag;

                                totalCount++;
                                if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    //TODO: Show also the MB/sec and total GB exported. // TODO arek
                                    Notifications.ShowProgress("Exported {0} files. ", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }
                            }
                        }

                        if (hasDocs == false)
                            break;

                    }
                }
                catch (Exception e)
                {
                    Notifications.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    Notifications.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalCount, state.LastFileEtag);

                    throw new SmugglerException(e.Message, e) { LastEtag = state.LastFileEtag };
                }
            }

            Notifications.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalCount, state.LastFileEtag);
        }

        private static RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata.Remove(SynchronizationConstants.RavenSynchronizationHistory);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationSource);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationVersion);
            }

            return metadata;
        }

        private static RavenJObject DisableVersioning(RavenJObject metadata)
        {
            metadata?.Add(Constants.RavenIgnoreVersioning, true);

            return metadata;
        }
    }
}