using System;
using System.IO;
using Raven.Server.ServerWide;
using Voron.Util.Settings;

namespace Raven.Server.Config.Settings
{
    public class PathSetting : PathSettingBase<PathSetting>
    {
        public PathSetting(string path, string baseDataDir = null)
            : base(path, baseDataDir != null ? new PathSetting(baseDataDir) : null)
        {
        }

        public PathSetting(string path, ResourceType type, string resourceName)
            : base(EnsureResourceInfo(path, type, resourceName))
        {
        }

        public override PathSetting Combine(string path)
        {
            return new PathSetting(Path.Combine(_path, path), _baseDataDir?.FullPath);
        }

        public override PathSetting Combine(PathSetting path)
        {
            return new PathSetting(Path.Combine(_path, path._path), _baseDataDir?.FullPath);
        }

        private static string EnsureResourceInfo(string path, ResourceType type, string name)
        {
            if (path == (string)RavenConfiguration.GetDefaultValue(x => x.Core.DataDirectory))
            {
                if (type == ResourceType.Server)
                    return "";

                return Path.Combine($"{type}s", name);
            }

            if (type == ResourceType.Server)
                return path;

            return Path.Combine(path, $"{type}s", name);
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ReadOnlyPathAttribute : Attribute
    {
    }
}
