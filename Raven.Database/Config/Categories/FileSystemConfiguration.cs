using System.Collections.Specialized;
using System.ComponentModel;
using System.IO;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Util;

namespace Raven.Database.Config.Categories
{
    public class FileSystemConfiguration : ConfigurationCategory
    {
        private readonly CoreConfiguration core;
        
        public FileSystemConfiguration(CoreConfiguration coreConfiguration)
        {
            core = coreConfiguration;
            Versioning = new FileSystemVersioningBundleConfiguration();
        }

        private string fileSystemDataDirectory;

        private string fileSystemIndexStoragePath;

        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/FileSystem/MaximumSynchronizationIntervalInSec")]
        [ConfigurationEntry("Raven/FileSystem/MaximumSynchronizationInterval")]
        public TimeSetting MaximumSynchronizationInterval { get; set; }

        /// <summary>
        /// The directory for the RavenDB file system. 
        /// You can use the ~\ prefix to refer to RavenDB's base directory. 
        /// </summary>
        [DefaultValue(@"~\FileSystems")]
        [ConfigurationEntry("Raven/FileSystem/DataDir")]
        public string DataDirectory
        {
            get { return fileSystemDataDirectory; }
            set { fileSystemDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(core.WorkingDirectory, value); }
        }

        [DefaultValue("")]
        [ConfigurationEntry("Raven/FileSystem/IndexStoragePath")]
        public string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(fileSystemIndexStoragePath))
                    fileSystemIndexStoragePath = Path.Combine(DataDirectory, "Indexes");
                return fileSystemIndexStoragePath;
            }
            set
            {
                fileSystemIndexStoragePath = value.ToFullPath();
            }
        }

        public FileSystemVersioningBundleConfiguration Versioning { get; }

        public override void Initialize(NameValueCollection settings)
        {
            base.Initialize(settings);

            Versioning.Initialize(settings);
        }
    }
}