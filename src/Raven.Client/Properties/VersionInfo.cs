using System;
using System.Linq;
using System.Reflection;

[assembly: Raven.Client.RavenVersion(Build = "{build}", CommitHash = "{commit}", Version = "4.0", FullVersionTemplate = "4.0.0-alpha-{build}")]

namespace Raven.Client
{
    [AttributeUsage(AttributeTargets.Assembly)]
    public class RavenVersionAttribute : Attribute
    {
        public string CommitHash { get; set; }
        public string Build { get; set; }
        public string Version { get; set; }
        public string FullVersionTemplate { get; set; }
        public string BuildType { get; set; }

        private static int? _buildVersion;

        private static RavenVersionAttribute _instance;

        public static RavenVersionAttribute Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = (RavenVersionAttribute)
                        typeof(RavenVersionAttribute).GetTypeInfo()
                            .Assembly.GetCustomAttributes(typeof(RavenVersionAttribute))
                            .Single();
                }

                return _instance;
            }
        }

        public int BuildVersion
        {
            get
            {
                if (_buildVersion == null)
                {
                    int _;
                    if (int.TryParse(Build, out _) == false)
                    {
                        _buildVersion = 40;
                    }
                    else
                    {
                        _buildVersion = _;
                    }
                }

                return _buildVersion.Value;
            }
        }

        public string FullVersion
        {
            get
            {
                return FullVersionTemplate.Replace("{build}", BuildVersion.ToString());
            }
        }
    }
}