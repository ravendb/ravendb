using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.Config;
using System.Linq;
using Sparrow.Platform;

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
                    var availableDrives = GetAvailableDrives();

                    if (PlatformDetails.RunningOnPosix == false)
                    {
                        // windows
                        folderPathOptions.List.UnionWith(availableDrives);
                    }
                    else
                    {
                        var list = new HashSet<string>();

                        foreach (var drive in availableDrives)
                        {
                           list.Add(Path.DirectorySeparatorChar + drive.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault());
                        }

                        folderPathOptions.List.UnionWith(list);
                    }
                }
                else if (Directory.Exists(path))
                {
                    path = path.Trim();

                    if (path.EndsWith("/") == false && path.EndsWith("\\") == false)
                        path += Path.DirectorySeparatorChar;

                    foreach (var directory in Directory.GetDirectories(path))
                    {
                        if (IsHiddenOrSystemDirectory(directory))
                            continue;
                        
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
                            if (directoryName.StartsWith(directoryPrefix, StringComparison.OrdinalIgnoreCase) &&
                                IsHiddenOrSystemDirectory(directory) == false)
                                folderPathOptions.List.Add(directory);
                        }
                    }
                    else if (string.IsNullOrEmpty(directoryPrefix) == false)
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

        private static bool IsHiddenOrSystemDirectory(string directory)
        {
            var dir = new DirectoryInfo(directory);
            return dir.Attributes.HasFlag(FileAttributes.Hidden | FileAttributes.System);
        }

        // DriveInfo.GetDrives() ends up in the native SystemNative_GetAllMountPoints, which calls getmntinfo().
        // getmntinfo() returns pointers into a libc-managed static buffer that is NOT thread-safe.
        // The Studio requests the folder-path options endpoint concurrently, so two parallel
        // DriveInfo.GetDrives() calls race on that shared buffer; one thread can marshal a clobbered
        // C-string and over-read freed/unmapped memory, producing a process-fatal AccessViolationException.
        // The defect is latent on macOS Sequoia (the race resolves benignly) and surfaces on macOS Tahoe,
        // whose mount-table changes widen the race window and reallocate the buffer.
        // See dotnet/runtime#122634 (fix: dotnet/runtime#122637, not yet backported to .NET 10).
        // We serialize the call on macOS so concurrent callers never enter getmntinfo() at the same time.
        private static readonly object MacOsDrivesLock = new object();

        private static List<string> GetAvailableDrives()
        {
            var list = new List<string>();

            var drives = GetDrives();
            foreach (var drive in drives)
            {
                list.Add(drive.RootDirectory.FullName);
            }

            return list;
        }

        private static DriveInfo[] GetDrives()
        {
            if (PlatformDetails.RunningOnMacOsx == false)
                return DriveInfo.GetDrives();

            lock (MacOsDrivesLock)
            {
                return DriveInfo.GetDrives();
            }
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
    public sealed class FolderPathOptions
    {
        public SortedSet<string> List { get; } = new SortedSet<string>();
    }
}
