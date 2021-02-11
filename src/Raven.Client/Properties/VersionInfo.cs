using System;
using System.Linq;
using System.Reflection;
using Raven.Client.Extensions;
using Raven.Client.Properties;

[assembly: RavenVersion(Build = "52", CommitHash = "a377982", Version = "5.2", FullVersion = "5.2.0-custom-52", ReleaseDateString = "2021-02-01")]

namespace Raven.Client.Properties
{
    [AttributeUsage(AttributeTargets.Assembly)]
    public class RavenVersionAttribute : Attribute
    {
        public string CommitHash { get; set; }
        public string Build { get; set; }
        public string Version { get; set; }
        public string FullVersion { get; set; }
        public string ReleaseDateString { get; set; }

        private static int? _buildVersion;
        private static readonly Version _assemblyVersion;
        private static string _assemblyVersionAsString;

        public static readonly RavenVersionAttribute Instance;

        static RavenVersionAttribute()
        {
            _assemblyVersion = typeof(RavenVersionAttribute).Assembly.GetName().Version;
            Instance = (RavenVersionAttribute)typeof(RavenVersionAttribute).Assembly.GetCustomAttributes(typeof(RavenVersionAttribute)).Single();
        }

        public RavenVersionAttribute()
        {
            MajorVersion = _assemblyVersion.Major;
            MajorVersionAsChar = char.Parse(MajorVersion.ToInvariantString());
            MinorVersion = _assemblyVersion.Minor;
            PatchVersion = _assemblyVersion.Build;
        }

        public string AssemblyVersion => _assemblyVersionAsString ?? (_assemblyVersionAsString = $"{MajorVersion.ToInvariantString()}.{MinorVersion.ToInvariantString()}.{PatchVersion.ToInvariantString()}.{BuildVersion.ToInvariantString()}");

        public readonly int MajorVersion;

        internal readonly char MajorVersionAsChar;

        public readonly int MinorVersion;

        public readonly int PatchVersion;

        internal DateTime ReleaseDate => DateTime.Parse(ReleaseDateString);

        public int BuildVersion
        {
            get
            {
                if (_buildVersion == null)
                {
                    if (string.IsNullOrWhiteSpace(Build))
                        throw new ArgumentNullException(nameof(Build));

                    _buildVersion = int.TryParse(Build, out var buildVersion)
                        ? buildVersion
                        : 52;
                }

                return _buildVersion.Value;
            }
        }
    }
}
