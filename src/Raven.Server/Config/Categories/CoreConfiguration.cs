using System;
using System.ComponentModel;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        private string _workingDirectory;
        private PathSetting _dataDirectory;

        private readonly RavenConfiguration _root;

        public CoreConfiguration(RavenConfiguration root)
        {
            _root = root;
        }

        [Description("The maximum allowed page size for queries")]
        [DefaultValue(1024)]
        [MinValue(10)]
        [ConfigurationEntry("Raven/MaxPageSize")]
        public int MaxPageSize { get; set; }

        [Description("The directory into which RavenDB will search the studio files, defaults to the base directory")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/StudioDirectory")]
        public string StudioDirectory { get; set; }

        [Description("The directory into which RavenDB will write the logs, for relative path, the applciation base directory is used")]
        [DefaultValue("Logs")]
        [ConfigurationEntry("Raven/LogsDirectory")]
        public string LogsDirectory { get; set; }

        [Description("The logs level which RavenDB will use (None, Information, Operations)")]
        [DefaultValue("Operations")]
        [ConfigurationEntry("Raven/LogsLevel")]
        public string LogLevel { get; set; }

        [Description("The URLs which the server should listen to. By default we listen to localhost:8080")]
        [DefaultValue("http://localhost:8080")]
        [ConfigurationEntry("Raven/ServerUrl")]
        public string ServerUrl { get; set; }

        [Description("If not specified, will use the server url host and random port. If it just a number specify, will use that port. Otherwise, will bind to the host & port specified")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/ServerUrl/Tcp")]
        public string TcpServerUrl { get; set; }

        [Description("Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory { get; set; }

        [DefaultValue(@"~\")]
        [ConfigurationEntry("Raven/WorkingDir")]
        public string WorkingDirectory
        {
            get { return _workingDirectory; }
            set { _workingDirectory = CalculateWorkingDirectory(value); }
        }

        [Description("The directory for the RavenDB resource. You can use the ~/ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(@"~/{pluralizedResourceType}/{name}")]
        [ConfigurationEntry("Raven/DataDir")]
        public PathSetting DataDirectory
        {
            get { return _dataDirectory; }
            set
            {
                _dataDirectory = value.ApplyParentPath(_root.ServerDataDir).ApplyWorkingDirectory(new PathSetting(WorkingDirectory));
            }
        }

        [Description("The time to wait before canceling a database operation such as load (many) or query")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/DatabaseOperationTimeoutInMin")]
        [LegacyConfigurationEntry("Raven/DatabaseOperationTimeout")]
        public TimeSetting DatabaseOperationTimeout { get; set; }

        [Description("Indicates if we should throw an exception if any index could not be opened")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/ThrowIfAnyIndexOrTransformerCouldNotBeOpened")]
        public bool ThrowIfAnyIndexOrTransformerCouldNotBeOpened { get; set; }

        [Description("Run as service")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunAsService")]
        public bool RunAsService { get; set; }

        private static string CalculateWorkingDirectory(string workingDirectory)
        {
            if (string.IsNullOrEmpty(workingDirectory))
                workingDirectory = @"~\";

            if (workingDirectory.StartsWith("APPDRIVE:", StringComparison.OrdinalIgnoreCase))
            {
                var baseDirectory = AppContext.BaseDirectory;
                var rootPath = Path.GetPathRoot(baseDirectory);
                if (string.IsNullOrEmpty(rootPath) == false)
                    workingDirectory = Regex.Replace(workingDirectory, "APPDRIVE:", rootPath.TrimEnd('\\'), RegexOptions.IgnoreCase);
            }

            return workingDirectory;
        }
    }
}