using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class BackupConfiguration : ConfigurationCategory
    {
        [Description("Local backups can only be created under this root path.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.LocalRootPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting LocalRootPath { get; set; }
        
        [Description("Semicolon seperated list of allowed backup destinations. If not specified, all destinations are allowed. Possible values: None, Local, Azure, AmazonGlacier, AmazonS3, FTP. Example list: \"Local;AmazonGlacier;AmazonS3\".")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.AllowedDestinations", ConfigurationEntryScope.ServerWideOnly)]
        public string[] AllowedDestinations { get; set; }

        [Description("Semicolon seperated list of allowed AWS regions. If not specified, all regions are allowed. Example list: \"ap-northeast-1;ap-northeast-2;ap-south-1\".")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.AllowedAwsRegions", ConfigurationEntryScope.ServerWideOnly)]
        public string[] AllowedAwsRegions { get; set; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);

            if (type != ResourceType.Server)
                return;

            ValidateLocalRootPath();
            ValidateAllowedDestinations();
            ValidateAllowedRegions();
        }

        private readonly HashSet<string> _allDestinations = new HashSet<string>
        {
            "None", "Local", "Azure", "AmazonGlacier", "AmazonS3", "FTP"
        };

        private void ValidateLocalRootPath()
        {
            if (LocalRootPath == null)
                return;

            var directoryInfo = new DirectoryInfo(LocalRootPath.FullPath);
            if (directoryInfo.Exists == false)
            {
                throw new ArgumentException($"The backup path '{LocalRootPath.FullPath}' defined in the configuration under '{RavenConfiguration.GetKey(x => x.Backup.LocalRootPath)}' doesn't exist.");
            }
        }
        
        private void ValidateAllowedDestinations()
        {
            if (AllowedDestinations == null)
                return;

            if (AllowedDestinations.Contains("None", StringComparer.OrdinalIgnoreCase))
            {
                if (AllowedDestinations.Length > 1)
                    throw new ArgumentException($"If you specify \"None\" under '{RavenConfiguration.GetKey(x => x.Backup.AllowedDestinations)}' then it must be the only value.");

                return;
            }

            foreach (var dest in AllowedDestinations)
            {
                if (_allDestinations.Contains(dest, StringComparer.OrdinalIgnoreCase))
                    continue;
                throw new ArgumentException($"The destination '{dest}' defined in the configuration under '{RavenConfiguration.GetKey(x => x.Backup.AllowedDestinations)}' is unknown. Make sure to use the following destinations: {string.Join(", ", _allDestinations)}.");
            }
        }

        private void ValidateAllowedRegions()
        {
            if (AllowedAwsRegions?.Length < 1)
                throw new ArgumentException($"The configuration value '{RavenConfiguration.GetKey(x => x.Backup.AllowedAwsRegions)}' cannot be empty. Either specify at least one allowed region or remove the configuration entry completely to allow all regions.");
        }

        public void AssertRegionAllowed(string region)
        {
            if (AllowedAwsRegions == null)
                return;
            if (AllowedAwsRegions.Contains(region))
                return;
            throw new ArgumentException($"The selected AWS region '{region}' is not allowed for backup in this RavenDB server. Contact the administrator for more information. Allowed regions: {string.Join(", ", AllowedAwsRegions)}");
        }

        public void AssertDestinationAllowed(string dest)
        {
            if (AllowedDestinations == null)
                return;
            if (AllowedDestinations.Contains("None"))
                throw new ArgumentException("Backups are not allowed in this RavenDB server. Contact the administrator for more information.");
            if (AllowedDestinations.Contains(dest))
                return;
            throw new ArgumentException($"The selected backup destination '{dest}' is not allowed in this RavenDB server. Contact the administrator for more information. Allowed backup destinations: {string.Join(", ", AllowedDestinations)}");

        }
    }
}
