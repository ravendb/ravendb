using System;
using System.IO;
using System.Reflection;
using Raven.Server.Config;
using Sparrow.Platform;
using Voron;
using Voron.Exceptions;
using Voron.Util.Settings;

namespace Raven.Server.Storage.Layout
{
    public static class StorageLoader
    {
        public static StorageEnvironment OpenEnvironment(StorageEnvironmentOptions options, StorageEnvironmentWithType.StorageEnvironmentType type, StorageEnvironmentSynchronization envWriteSync = null)
        {
            try
            {
                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions directoryOptions)
                    return OpenEnvironmentWithPossibleLayoutUpdate(directoryOptions, type, envWriteSync);        

                return new StorageEnvironment(options, envWriteSync);
            }
            catch (Exception)
            {
                options.Dispose();

                throw;
            }
        }

        private static StorageEnvironment OpenEnvironmentWithPossibleLayoutUpdate(StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions options, StorageEnvironmentWithType.StorageEnvironmentType type, StorageEnvironmentSynchronization envWriteSync)
        {
            try
            {
                var oldOwnsPager = options.OwnsPagers;
                options.OwnsPagers = false;
                try
                {
                    return new StorageEnvironment(options, envWriteSync);
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
                var journalsPath = options.JournalPath;
                if (Directory.Exists(preRtmJournalPath.FullPath))
                {
                    TryMoveJournals(preRtmJournalPath, journalsPath);
                    Directory.Delete(preRtmJournalPath.FullPath);
                }
                else
                {
                    if (TryMoveJournals(basePath, journalsPath) == false)
                    {
                        var message =
                            $"Failed to open a storage at {options} due to invalid or missing journal files. In order to load the storage successfully we need all journals to be not corrupted. ";

                        string ravenServer = Assembly.GetAssembly(typeof(StorageLoader)).GetName().Name + (PlatformDetails.RunningOnPosix ? string.Empty : ".exe");

                        switch (type)
                        {
                            case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                                message += "The recommended approach is to reset the index in order to recover from this error. Alternatively you can temporarily start the server in " +
                                           $"dangerous mode so it will ignore invalid journals on startup: {Environment.NewLine}" +
                                           $"{ravenServer} --{RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors)}=true{Environment.NewLine}" +
                                           "This switch is meant to be use only for recovery purposes. Please make sure that you won't use it after you manage to recover your data. " +
                                           "Eventually you should reset the index anyway to be sure you won't experience any invalid data errors in the future.";
                                break;
                            case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                                message +=
                                    "You can load a database by starting the server in dangerous mode temporarily so it will ignore invalid journals on startup but you need to " +
                                    $"export and import database immediately afterwards: {Environment.NewLine}" +
                                    $"{ravenServer} --{RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors)}=true{Environment.NewLine}" +
                                    "This switch is meant to be use only for recovery purposes. Please make sure that you won't use it after you manage to recover your data. " +
                                    "If you won't be able to export the database successfully you need restore the database from the backup or use Voron.Recovery tool. ";
                                break;
                            case StorageEnvironmentWithType.StorageEnvironmentType.Configuration:
                                message += $"You can delete the configuration storage folder at '{basePath.FullPath}' and restart the server. It will be recreated on database startup." +
                                           "This storage contains only notifications and alerts that you see in RavenDB studio";
                                break;
                            case StorageEnvironmentWithType.StorageEnvironmentType.System:
                                message += "You can start the server in dangerous mode temporarily so it will ignore invalid journals on startup:" +
                                           $"{ravenServer} --{RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors)}=true{Environment.NewLine}" +
                                           "This switch is meant to be use only for recovery purposes. Please make sure that you won't use it after you manage to recover your data. " +
                                           $"Eventually you should delete the system storage at '{basePath.FullPath}', start the server and create your databases again with the usage of existing data.";
                                break;
                            default:
                                throw new ArgumentException($"Unknown storage type: {type}", nameof(type));

                        }

                        throw new InvalidJournalException($"{message}{Environment.NewLine}Error details: {e.Message}");
                    }
                }

                return new StorageEnvironment(options, envWriteSync);
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

                BackupAndDeleteFile(destination);

                File.Move(source.FullPath, destination.FullPath);
            }

            return true;
        }

        private static void BackupAndDeleteFile(VoronPathSetting path)
        {
            if (File.Exists(path.FullPath) == false)
                return;

            var count = 0;
            while (true)
            {
                var filePath = $"{path.FullPath}.{count++}.bak";
                if (File.Exists(filePath))
                    continue;

                File.Move(path.FullPath, filePath);
                break;
            }
        }
    }
}
