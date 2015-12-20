// -----------------------------------------------------------------------
//  <copyright file="FileSystemStreamDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;

namespace Raven.Smuggler.FileSystem.Streams
{
    public class StreamSmugglingDestination : IFileSystemSmugglerDestination
    {
        private readonly Stream stream;

        private readonly bool leaveOpen;

        private PositionWrapperStream positionStream;

        private ZipArchive archive;

        public StreamSmugglingDestination(Stream stream, bool leaveOpen = false)
        {
            this.stream = stream;
            this.leaveOpen = leaveOpen;
        }

        public Task InitializeAsync(FileSystemSmugglerOptions options, FileSystemSmugglerNotifications notifications, CancellationToken cancellationToken)
        {
            // We use PositionWrapperStream due to:
            // http://connect.microsoft.com/VisualStudio/feedbackdetail/view/816411/ziparchive-shouldnt-read-the-position-of-non-seekable-streams
            positionStream = new PositionWrapperStream(stream, leaveOpen: true);
            archive = new ZipArchive(positionStream, ZipArchiveMode.Create, leaveOpen: true);

            return new CompletedTask();
        }

        public ISmuggleFilesToDestination WriteFiles()
        {
            return new SmuggleFilesToStream(archive);
        }

        public ISmuggleConfigurationsToDestination WriteConfigurations()
        {
            return new SmuggleConfigurationsToStream(archive);
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
            archive.Dispose();
            positionStream.Dispose();

            if (leaveOpen)
                return;

            stream?.Flush();
            stream?.Dispose();
        }
    }
}