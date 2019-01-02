using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.Config;

namespace Raven.Server.Web.Studio
{
    public static class FolderPath
    {
        public static FolderPathOptions GetOptions(string path, bool isBackupFolder, RavenConfiguration ravenConfiguration)
        {
            var folderPathOptions = new FolderPathOptions();

            try
            {
                var restrictedFolder = GetRestrictedFolder(isBackupFolder, ravenConfiguration);
                if (restrictedFolder != null)
                {
                    folderPathOptions.List.Add(restrictedFolder);
                }
                else if (string.IsNullOrWhiteSpace(path))
                {
                    folderPathOptions.List.AddRange(GetAvailableDrives());
                }
                else if (Directory.Exists(path))
                {
                    path = path.Trim();

                    if (path.EndsWith("/") == false && path.EndsWith("\\") == false)
                        path = Path.Combine(path, Path.DirectorySeparatorChar.ToString());

                    foreach (var directory in Directory.GetDirectories(path))
                    {
                        folderPathOptions.List.Add(directory);
                    }
                }
                else
                {
                    path = path.Trim();

                    // prefix of a directory
                    var directoryPrefix = Path.GetFileName(path);
                    var directoryPath = Path.GetDirectoryName(path);
                    if (Directory.Exists(directoryPath))
                    {
                        foreach (var directory in Directory.GetDirectories(directoryPath))
                        {
                            var directoryName = Path.GetFileName(directory);
                            if (directoryName.StartsWith(directoryPrefix, StringComparison.OrdinalIgnoreCase))
                                folderPathOptions.List.Add(directory);
                        }
                    }
                    else
                    {
                        var availableDrives = GetAvailableDrives();
                        foreach (var drive in availableDrives)
                        {
                            if (drive.StartsWith(directoryPrefix, StringComparison.OrdinalIgnoreCase))
                                folderPathOptions.List.Add(drive);
                        }
                    }
                }
            }
            catch
            {
                // nothing we can do here
            }

            return folderPathOptions;
        }

        private static List<string> GetAvailableDrives()
        {
            var list = new List<string>();

            var drives = DriveInfo.GetDrives();
            foreach (var drive in drives)
            {
                list.Add(drive.RootDirectory.FullName);
            }

            return list;
        }

        private static string GetRestrictedFolder(bool isBackupFolder, RavenConfiguration ravenConfiguration)
        {
            if (isBackupFolder)
            {
                return ravenConfiguration.Backup.LocalRootPath?.FullPath;
            }

            if (ravenConfiguration.Core.EnforceDataDirectoryPath)
            {
                return ravenConfiguration.Core.DataDirectory?.FullPath;
            }

            return null;
        }
    }

    public class FolderPathOptions
    {
        public List<string> List { get; } = new List<string>();
    }
}
