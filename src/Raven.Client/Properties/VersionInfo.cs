using System;
using System.Linq;
using System.Reflection;
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

        private static RavenVersionAttribute _instance;

        public static RavenVersionAttribute Instance => _instance ?? (_instance = (RavenVersionAttribute)typeof(RavenVersionAttribute).GetTypeInfo().Assembly.GetCustomAttributes(typeof(RavenVersionAttribute)).Single());

        public int BuildVersion
        {
            get
            {
                if (_buildVersion == null)
                {
                    int _;
                    if (int.TryParse(Build, out _) == false)
                    {
                        _buildVersion = 41;
                    }
                    else
                    {
                        _buildVersion = _;
                    }
                }

                return _buildVersion.Value;
            }
        }
    }
}
