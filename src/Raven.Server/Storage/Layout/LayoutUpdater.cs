﻿using System;
using System.IO;
using Voron;
using Voron.Exceptions;
using Voron.Util.Settings;

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
            catch (InvalidJournalException)
            {
                var basePath = options.BasePath;
                var preRtmJournalPath = basePath.Combine("Journal");
                var journalsPath = options.JournalPath;
                if (Directory.Exists(preRtmJournalPath.FullPath))
                {
                    TryMoveJournals(preRtmJournalPath, journalsPath);
                    Directory.Delete(preRtmJournalPath.FullPath);
                }
                else
                {
                    if (TryMoveJournals(basePath, journalsPath) == false)
                        throw;
                }

                return new StorageEnvironment(options);
            }
        }

        private static bool TryMoveJournals(VoronPathSetting basePath, VoronPathSetting journalsPath)
        {
            var journalsInRoot = Directory.GetFiles(basePath.FullPath, "*.journal", SearchOption.TopDirectoryOnly);
            if (journalsInRoot.Length == 0)
                return false;

            foreach (var journalFile in journalsInRoot)
            {
                var journalFileInfo = new FileInfo(journalFile);

                var source = basePath.Combine(journalFileInfo.Name);
                var destination = journalsPath.Combine(journalFileInfo.Name);

                if (File.Exists(destination.FullPath))
                    File.Delete(destination.FullPath);

                File.Move(source.FullPath, destination.FullPath);
            }

            return true;
        }
    }
}
