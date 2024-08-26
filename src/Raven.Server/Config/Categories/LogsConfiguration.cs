using System.Collections.Generic;
using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Logs)]
    public sealed class LogsConfiguration : ConfigurationCategory
    {
        [DefaultValue(null)]
        [ConfigurationEntry("Logs.ConfigPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting ConfigPath { get; set; }

        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting Path { get; set; }

        [DefaultValue(LogLevel.Info)]
        [ConfigurationEntry("Logs.MinLevel", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel MinLevel { get; set; }

        [DefaultValue(LogLevel.Fatal)]
        [ConfigurationEntry("Logs.MaxLevel", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel MaxLevel { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Logs.NLog.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting NLogInternalPath { get; set; }

        [DefaultValue(LogLevel.Info)]
        [ConfigurationEntry("Logs.NLog.Level", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel NLogInternalLevel { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Logs.NLog.LogToConsole", ConfigurationEntryScope.ServerWideOnly)]
        public bool NLogLogToConsole { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Logs.NLog.LogToConsoleError", ConfigurationEntryScope.ServerWideOnly)]
        public bool NLogLogToConsoleError { get; set; }

        [DefaultValue(128)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.ArchiveAboveSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size ArchiveAboveSize { get; set; }

        [DefaultValue(3)]
        [ConfigurationEntry("Logs.MaxArchiveDays", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxArchiveDays { get; set; }

        [DefaultValue(null)]
        [MinValue(0)]
        [ConfigurationEntry("Logs.MaxArchiveFiles", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxArchiveFiles { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Logs.EnableArchiveFileCompression", ConfigurationEntryScope.ServerWideOnly)]
        public bool EnableArchiveFileCompression { get; set; }

        [DefaultValue(LogLevel.Error)]
        [ConfigurationEntry("Logs.Microsoft.MinLevel", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel MicrosoftMinLevel { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Logs.ThrowExceptions", ConfigurationEntryScope.ServerWideOnly)]
        public bool ThrowExceptions { get; set; }

        [Description("Location of NuGet packages cache")]
        [DefaultValue("Logs/NuGet")]
        [ConfigurationEntry("Logs.NuGetPackagesPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting NuGetPackagesPath { get; set; }

        [Description("Default NuGet source URL")]
        [DefaultValue("https://api.nuget.org/v3/index.json")]
        [ConfigurationEntry("Logs.NuGetPackageSourceUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string NuGetPackageSourceUrl { get; set; }

        [Description("Allow installation of NuGet prerelease packages")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.NuGetAllowPreReleasePackages", ConfigurationEntryScope.ServerWideOnly)]
        public bool NuGetAllowPreReleasePackages { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Logs.NuGetAdditionalPackages", ConfigurationEntryScope.ServerWideOnly)]
        public Dictionary<string, string> NuGetAdditionalPackages { get; set; }
    }
}
