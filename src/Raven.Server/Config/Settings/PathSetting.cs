using System;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.ServerWide;
using Sparrow.Platform;
using Voron.Platform.Posix;

namespace Raven.Server.Config.Settings
{
    public class PathSetting
    {
        private readonly string _path;

        private string _fullPath;

        public PathSetting(string path)
        {
            _path = HandleAppDriveIfAbsolutePath(path);
        }

        public PathSetting(string path, ResourceType type, string resourceName)
        {
            _path = HandleAppDriveIfAbsolutePath(EnsureResourceInfo(path, type, resourceName));
        }

        public PathSetting(PathSetting path)
        {
            _path = path._path;
        }

        public string FullPath => _fullPath ?? (_fullPath =  ToFullPath(_path));

        public PathSetting Combine(string path)
        {
            return new PathSetting(Path.Combine(_path, path));
        }

        public PathSetting Combine(PathSetting path)
        {
            return new PathSetting(Path.Combine(_path, path._path));
        }

        public static string ToFullPath(string path)
        {
            path = Environment.ExpandEnvironmentVariables(path);

            if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
                path = Path.Combine(AppContext.BaseDirectory, path.Substring(2));

            var result = Path.IsPathRooted(path) ? path : Path.Combine(AppContext.BaseDirectory, path);

            if (PlatformDetails.RunningOnPosix)
                return PosixHelper.FixLinuxPath(result);

            return Path.GetFullPath(result); // it will unify directory separators
        }

        private static string EnsureResourceInfo(string path, ResourceType type, string name)
        {
            if (path == (string)RavenConfiguration.GetDefaultValue(x => x.Core.DataDirectory))
            {
                if (type == ResourceType.Server)
                    return $"~{Path.DirectorySeparatorChar}";

                return Path.Combine("~", $"{type}s", name);
            }

            if (type == ResourceType.Server)
                return path;

            return Path.Combine(path, $"{type}s", name);
        }

        private static string HandleAppDriveIfAbsolutePath(string path)
        {
            if (path.StartsWith("APPDRIVE:", StringComparison.OrdinalIgnoreCase))
            {
                var baseDirectory = AppContext.BaseDirectory;
                var rootPath = Path.GetPathRoot(baseDirectory);

                if (string.IsNullOrEmpty(rootPath) == false)
                    return Regex.Replace(path, "APPDRIVE:", rootPath.TrimEnd('\\'), RegexOptions.IgnoreCase);
            }

            return path;
        }

        public override string ToString()
        {
            return FullPath;
        }
    }
}
