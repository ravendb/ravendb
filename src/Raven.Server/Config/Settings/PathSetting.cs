using System;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.ServerWide;
using Voron.Util.Settings;

namespace Raven.Server.Config.Settings
{
    public class PathSetting : PathSettingBase<PathSetting>
    {
        public PathSetting(string path, string baseDataDir = null)
            : base(HandleAppDriveIfAbsolutePath(path), baseDataDir != null ? new PathSetting(baseDataDir) : null)
        {
        }

        public PathSetting(string path, ResourceType type, string resourceName)
            : base(HandleAppDriveIfAbsolutePath(EnsureResourceInfo(path, type, resourceName)))
        {
        }

        public override PathSetting Combine(string path)
        {
            return new PathSetting(Path.Combine(_path, path));
        }

        public override PathSetting Combine(PathSetting path)
        {
            return new PathSetting(Path.Combine(_path, path._path));
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
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ReadOnlyPathAttribute : Attribute
    {
    }
}
