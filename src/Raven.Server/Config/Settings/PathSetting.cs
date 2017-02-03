using System;
using System.IO;
using System.Linq;
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
            _path = path;
        }

        public PathSetting(string path, ResourceType type, string resourceName)
        {
            _path = EnsureResourceInfo(path, type, resourceName);
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

        public PathSetting ApplyWorkingDirectory(PathSetting workingDirectory)
        {
            if (string.IsNullOrEmpty(workingDirectory._path))
                return this;

            if (Path.IsPathRooted(_path))
                return this;

            if (_path.StartsWith(@"~/") || _path.StartsWith(@"~\"))
            {
                return new PathSetting(_path
                    .Replace(@"~/", workingDirectory._path)
                    .Replace(@"~\", workingDirectory._path));
            }

            return Combine(workingDirectory);
        }

        public PathSetting ApplyParentPath(PathSetting parent)
        {
            if (Path.IsPathRooted(_path))
                return this;

            if (parent == null)
                return this;

            var path = Environment.ExpandEnvironmentVariables(parent._path);

            if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
                return this;

            return parent.Combine(this);
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

        public override string ToString()
        {
            return FullPath;
        }
    }
}
