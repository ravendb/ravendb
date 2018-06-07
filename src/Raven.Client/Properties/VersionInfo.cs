using System;
using System.Linq;
using System.Reflection;
using Raven.Client.Extensions;
using Raven.Client.Properties;

[assembly: RavenVersion(Build = "41", CommitHash = "a377982", Version = "4.1", FullVersion = "4.1.0-custom-41")]

namespace Raven.Client.Properties
{
    [AttributeUsage(AttributeTargets.Assembly)]
    public class RavenVersionAttribute : Attribute
    {
        public string CommitHash { get; set; }
        public string Build { get; set; }
        public string Version { get; set; }
        public string FullVersion { get; set; }

        private static int? _buildVersion;
        private static readonly Version _assemblyVersion;

        public static readonly RavenVersionAttribute Instance;

        static RavenVersionAttribute()
        {
            _assemblyVersion = typeof(RavenVersionAttribute).GetTypeInfo().Assembly.GetName().Version;
            Instance = (RavenVersionAttribute)typeof(RavenVersionAttribute).GetTypeInfo().Assembly.GetCustomAttributes(typeof(RavenVersionAttribute)).Single();
        }

        public RavenVersionAttribute()
        {
            MajorVersion = _assemblyVersion.Major;
            MinorVersion = _assemblyVersion.Minor;
            PatchVersion = _assemblyVersion.Build;
            AssemblyVersion = $"{MajorVersion.ToInvariantString()}.{MinorVersion.ToInvariantString()}.{PatchVersion.ToInvariantString()}.{BuildVersion.ToInvariantString()}";
        }

        public readonly string AssemblyVersion;

        public readonly int MajorVersion;

        public readonly int MinorVersion;

        public readonly int PatchVersion;

        public int BuildVersion
        {
            get
            {
                if (_buildVersion == null)
                {
                    if (int.TryParse(Build, out var buildVersion) == false)
                    {
                        _buildVersion = 41;
                    }
                    else
                    {
                        _buildVersion = buildVersion;
                    }
                }

                return _buildVersion.Value;
            }
        }
    }
}
