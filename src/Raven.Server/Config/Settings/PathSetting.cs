using System;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.ServerWide;
using Sparrow.Platform;
using Voron.Util.Settings;

namespace Raven.Server.Config.Settings
{
    public class PathSetting : PathSettingBase<PathSetting>
    {
        public PathSetting(string path, string baseDataDir = null)
            : base(ExpandConstantsInPath(path), baseDataDir != null ? new PathSetting(baseDataDir) : null)
        {
        }

        public PathSetting(string path, ResourceType type, string resourceName)
            : base(ExpandConstantsInPath(EnsureResourceInfo(path, type, resourceName)))
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

        private static string ExpandConstantsInPath(string path)
        {
            if (path.StartsWith("APPDRIVE:", StringComparison.OrdinalIgnoreCase))
            {
                var baseDirectory = AppContext.BaseDirectory;
                var rootPath = Path.GetPathRoot(baseDirectory);

                if (string.IsNullOrEmpty(rootPath) == false)
                    return Regex.Replace(path, "APPDRIVE:", rootPath.TrimEnd('\\'), RegexOptions.IgnoreCase);
            }


            if (PlatformDetails.RunningOnPosix && path.StartsWith("$HOME", StringComparison.OrdinalIgnoreCase))
            {
                var homeDir = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                if (string.IsNullOrEmpty(homeDir) == false)
                    return path.Replace("$HOME", homeDir.TrimEnd('/'), StringComparison.OrdinalIgnoreCase);
            }

            return path;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ReadOnlyPathAttribute : Attribute
    {
    }
}
