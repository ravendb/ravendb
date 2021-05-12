using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Backup)]
    public class BackupConfiguration : ConfigurationCategory
    {
        [Description("Local backups can only be created under this root path.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.LocalRootPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting LocalRootPath { get; set; }

        [Description("You can use this setting to specify a different path to the temporary backup files (used when the local destination isn't specified). By default it is empty, which means that temporary files will be created at the same location as the data file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.TempPath", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public PathSetting TempPath { get; set; }

        [Description("Semicolon seperated list of allowed backup destinations. If not specified, all destinations are allowed. Possible values: None, Local, Azure, AmazonGlacier, AmazonS3,GoogleCloud , FTP. Example list: \"Local;AmazonGlacier;AmazonS3\".")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.AllowedDestinations", ConfigurationEntryScope.ServerWideOnly)]
        public string[] AllowedDestinations { get; set; }

        [Description("Semicolon seperated list of allowed AWS regions. If not specified, all regions are allowed. Example list: \"ap-northeast-1;ap-northeast-2;ap-south-1\".")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.AllowedAwsRegions", ConfigurationEntryScope.ServerWideOnly)]
        public string[] AllowedAwsRegions { get; set; }

        [Description("Maximum number of concurrent backup tasks")]
        [DefaultValue(null)]
        [ConfigurationEntry("Backup.MaxNumberOfConcurrentBackups", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxNumberOfConcurrentBackups { get; set; }

        [Description("Number of seconds to delay the backup after hitting the maximum number of concurrent backups limit")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Backup.ConcurrentBackupsDelayInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ConcurrentBackupsDelay { get; set; }

        [Description("Number of minutes to delay the backup after entering the low memory state by the Server.")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Backup.LowMemoryBackupDelayInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting LowMemoryBackupDelay { get; set; }

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);

            if (type != ResourceType.Server)
                return;

            ValidateLocalRootPath();
            ValidateAllowedDestinations();
            ValidateAllowedRegions();
        }

        internal static readonly HashSet<string> _allDestinations =
            new HashSet<string>(Enum.GetValues(typeof(PeriodicBackupConfiguration.BackupDestination))
                .Cast<PeriodicBackupConfiguration.BackupDestination>().Select(x => x.ToString()), OrdinalIgnoreCaseStringStructComparer.Instance);

        private const string _noneDestination = nameof(PeriodicBackupConfiguration.BackupDestination.None);

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
        
        internal void ValidateAllowedDestinations()
        {
            if (AllowedDestinations == null)
                return;

            if (AllowedDestinations.Contains(_noneDestination, StringComparer.OrdinalIgnoreCase))
            {
                if (AllowedDestinations.Length > 1)
                    throw new ArgumentException($"If you specify \"None\" under '{RavenConfiguration.GetKey(x => x.Backup.AllowedDestinations)}' then it must be the only value.");

                return;
            }

            foreach (var dest in AllowedDestinations)
            {
                if (_allDestinations.Contains(dest))
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

            if (AllowedDestinations.Contains(_noneDestination, StringComparer.OrdinalIgnoreCase))
                throw new ArgumentException("Backups are not allowed in this RavenDB server. Contact the administrator for more information.");

            if (AllowedDestinations.Contains(dest, StringComparer.OrdinalIgnoreCase))
                return;

            throw new ArgumentException($"The selected backup destination '{dest}' is not allowed in this RavenDB server. Contact the administrator for more information. Allowed backup destinations: {string.Join(", ", AllowedDestinations)}");
        }
    }
}
