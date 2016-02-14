using System;
using System.ComponentModel;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        private bool runInMemory;
        private string workingDirectory;
        private string dataDirectory;
        private string serverUrl;

        [Description("The maximum allowed page size for queries")]
        [DefaultValue(1024)]
        [MinValue(10)]
        [ConfigurationEntry("Raven/MaxPageSize")]
        public int MaxPageSize { get; set; }

        [Description("The URLs which the server should listen to. By default we listen to localhost:8080")]
        [DefaultValue("http://localhost:8080")]
        [ConfigurationEntry("Raven/ServerUrl")]
        public string ServerUrl
        {
            get { return serverUrl; }
            set
            {
                serverUrl = value;
            }
        }

        [Description("Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory
        {
            get { return runInMemory; }
            set
            {
                runInMemory = value;
            }
        }

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