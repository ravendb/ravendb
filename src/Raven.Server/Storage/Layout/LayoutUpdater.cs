using System;
using System.IO;
using Voron;
using Voron.Exceptions;

namespace Raven.Server.Storage.Layout
{
    public static class LayoutUpdater
    {
        public static StorageEnvironment OpenEnvironment(StorageEnvironmentOptions options)
        {
            try
            {
                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions directoryOptions)
                    return OpenEnvironmentInternal(directoryOptions);

                return new StorageEnvironment(options);
            }
            catch (Exception)
            {
                options.Dispose();

                throw;
            }
        }

        private static StorageEnvironment OpenEnvironmentInternal(StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions options)
        {
            try
            {
                var oldOwnsPager = options.OwnsPagers;
                options.OwnsPagers = false;
                try
                {
                    return new StorageEnvironment(options);
                }
                finally
                {
                    options.OwnsPagers = oldOwnsPager;
                }
            }
            catch (InvalidJournalException e)
            {
                var basePath = options.BasePath;
                var preRtmJournalPath = basePath.Combine("Journal");
                if (Directory.Exists(preRtmJournalPath.FullPath))
                    throw new InvalidOperationException(
                        "We could not find a journal file, but we have detected that you might have a pre-RTM directory layout. Please move all journals from 'Journal' directory to directory where '.voron' file is and reload the database.",
                        e);

                var journalsInRoot = Directory.GetFiles(basePath.FullPath, "*.journal", SearchOption.TopDirectoryOnly);
                if (journalsInRoot.Length == 0)
                    throw;

                var journalsPath = options.JournalPath;

                foreach (var journalFile in journalsInRoot)
                {
                    var journalFileInfo = new FileInfo(journalFile);

                    var source = basePath.Combine(journalFileInfo.Name);
                    var destination = journalsPath.Combine(journalFileInfo.Name);

                    File.Move(source.FullPath, destination.FullPath);
                }

                return new StorageEnvironment(options);
            }
        }
    }
}
