using System;
using System.ComponentModel;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        private string workingDirectory;
        private string dataDirectory;

        [Description("The maximum allowed page size for queries")]
        [DefaultValue(1024)]
        [MinValue(10)]
        [ConfigurationEntry("Raven/MaxPageSize")]
        public int MaxPageSize { get; set; }

        [Description("The URLs which the server should listen to. By default we listen to all network interfaces at port 8080")]
        [DefaultValue("http://0.0.0.0:8080")]
        [ConfigurationEntry("Raven/ServerUrl")]
        public string ServerUrl { get; set; }

        [Description("The URLs which the server should listen to. By default we listen to localhost:8081")]
        [DefaultValue("tcp://localhost:0")]
        [ConfigurationEntry("Raven/ServerUrl/TCP")]
        public string TcpServerUrl { get; set; }

        [Description("Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory { get; set; }

        [DefaultValue(@"~\")]
        [ConfigurationEntry("Raven/WorkingDir")]
        public string WorkingDirectory
        {
            get { return workingDirectory; }
            set { workingDirectory = CalculateWorkingDirectory(value); }
        }

        [Description("The directory for the RavenDB database. You can use the ~\\ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(@"~\Databases\System")]
        [ConfigurationEntry("Raven/DataDir")]
        public string DataDirectory
        {
            get { return dataDirectory; }
            set { dataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value); }
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

            return FilePathTools.MakeSureEndsWithSlash(workingDirectory.ToFullPath());
        }
    }
}