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
        [Description("A path to an XML file that overrides all NLog configuration")]
        [DefaultValue(null)]
        [ConfigurationEntry("Logs.ConfigPath", ConfigurationEntryScope.ServerWideOnly)]
        [ReadOnlyPath]
        public PathSetting ConfigPath { get; set; }

        [Description("The path to a folder that log files are written to")]
        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting Path { get; set; }

        [Description("Determines the minimal logging level.")]
        [DefaultValue(LogLevel.Info)]
        [ConfigurationEntry("Logs.MinLevel", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel MinLevel { get; set; }

        [Description("The path to a folder that NLog internal-usage logs are written to")]
        [DefaultValue(null)]
        [ConfigurationEntry("Logs.Internal.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting NLogInternalPath { get; set; }

        [Description("Determines the logging level for NLog internal-usage logs.")]
        [DefaultValue(LogLevel.Info)]
        [ConfigurationEntry("Logs.Internal.Level", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel NLogInternalLevel { get; set; }

        [Description("Determines whether to write NLog internal-usage messages to the standard output stream.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.Internal.LogToStandardOutput", ConfigurationEntryScope.ServerWideOnly)]
        public bool NLogInternalLogToStandardOutput { get; set; }

        [Description("Determines whether to write NLog internal-usage messages to the standard output error stream.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.Internal.LogToStandardError", ConfigurationEntryScope.ServerWideOnly)]
        public bool NLogInternalLogToStandardError { get; set; }

        [Description("The largest size (in megabytes) that a log file may reach " +
                     "before it is archived and logging is directed to a new file.")]
        [DefaultValue(128)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.ArchiveAboveSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size ArchiveAboveSize { get; set; }

        [Description("The maximum number of days that an archived log file is kept")]
        [DefaultValue(3)]
        [ConfigurationEntry("Logs.MaxArchiveDays", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxArchiveDays { get; set; }

        [Description("The maximum number of archived log files to keep")]
        [DefaultValue(null)]
        [MinValue(0)]
        [ConfigurationEntry("Logs.MaxArchiveFiles", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaxArchiveFiles { get; set; }

        [Description("Determines whether to compress archived log files.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.EnableArchiveFileCompression", ConfigurationEntryScope.ServerWideOnly)]
        [ConfigurationEntry("Logs.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool EnableArchiveFileCompression { get; set; }

        [Description("The minimal logging level for Microsoft logs")]
        [DefaultValue(LogLevel.Error)]
        [ConfigurationEntry("Logs.Microsoft.MinLevel", ConfigurationEntryScope.ServerWideOnly)]
        public LogLevel MicrosoftMinLevel { get; set; }

        [Description("Determines whether to throw an exception if NLog detects a logging configuration error.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.ThrowConfigExceptions", ConfigurationEntryScope.ServerWideOnly)]
        public bool ThrowConfigExceptions { get; set; }

        [Description("Location of NuGet packages cache")]
        [DefaultValue("Packages/NuGet/Logging")]
        [ConfigurationEntry("Logs.NuGet.PackagesPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting NuGetPackagesPath { get; set; }

        [Description("Default NuGet source URL")]
        [DefaultValue("https://api.nuget.org/v3/index.json")]
        [ConfigurationEntry("Logs.NuGet.PackageSourceUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string NuGetPackageSourceUrl { get; set; }

        [Description("Determines whether to allow installation of NuGet prerelease packages.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.NuGet.AllowPreReleasePackages", ConfigurationEntryScope.ServerWideOnly)]
        public bool NuGetAllowPreReleasePackages { get; set; }

        [Description("Additional Nuget packages to load during server startup for additional logging targets.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Logs.NuGet.AdditionalPackages", ConfigurationEntryScope.ServerWideOnly)]
        public Dictionary<string, string> NuGetAdditionalPackages { get; set; }
    }
}
