using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Raven.Server.ServerWide;
using Sparrow.Platform;
using Voron.Platform.Posix;

namespace Raven.Server.Config.Settings
{
    public class PathSetting
    {
        private readonly PathSetting _baseDataDir;
        private readonly string _path;

        private string _fullPath;

        public PathSetting(string path, string baseDataDir = null)
        {
            _baseDataDir = baseDataDir != null ? new PathSetting(baseDataDir) : null;
            _path = HandleAppDriveIfAbsolutePath(path);
        }

        public PathSetting(string path, ResourceType type, string resourceName)
        {
            _path = HandleAppDriveIfAbsolutePath(EnsureResourceInfo(path, type, resourceName));
        }

        public string FullPath => _fullPath ?? (_fullPath =  ToFullPath());

        public PathSetting Combine(string path)
        {
            return new PathSetting(Path.Combine(_path, path));
        }

        public PathSetting Combine(PathSetting path)
        {
            return new PathSetting(Path.Combine(_path, path._path));
        }

        public string ToFullPath()
        { 
            var path = Environment.ExpandEnvironmentVariables(_path);

            if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
                path = Path.Combine(_baseDataDir?.FullPath ?? AppContext.BaseDirectory, path.Substring(2));

            var result = Path.IsPathRooted(path)
                ? path
                : Path.Combine(_baseDataDir?.FullPath ?? AppContext.BaseDirectory, path);

            if (result.Length > 260 && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                result = @"\\?\" + result;

            if (result.EndsWith(@"\") || result.EndsWith(@"/"))
                result = result.TrimEnd('\\', '/');

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
