using System.IO;
using FastTests.Utils;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6141 : RavenTestBase
    {
        private readonly string _rootPathString = LinuxTestUtils.RunningOnPosix ? @"/" : @"C:\";

        private readonly string _emptySettingsDir;
        private readonly string _emptySettingFile;

        public RavenDB_6141()
        {
            _emptySettingsDir = NewDataPath(prefix: nameof(RavenDB_6141));
            _emptySettingFile = Path.Combine(_emptySettingsDir, "settings.json");

            if (Directory.Exists(_emptySettingsDir))
                IOExtensions.DeleteDirectory(_emptySettingsDir);

            Directory.CreateDirectory(_emptySettingsDir);

            using (var f = File.CreateText(_emptySettingFile))
                f.Write("{}");
        }

        [Fact]
        public void Default_database_path_settings()
        {
            var config = RavenConfiguration.CreateForTesting("foo", ResourceType.Database, _emptySettingFile);

            config.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            config.Initialize();

            Assert.Equal(new PathSetting("Databases/foo").FullPath, config.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("Databases/foo/Indexes").FullPath, config.Indexing.StoragePath.FullPath);

            Assert.Null(config.Indexing.TempPath);

            Assert.Null(config.Storage.TempPath);

            // actual configuration is created in the following manner

            config = RavenConfiguration.CreateForDatabase(RavenConfiguration.CreateForServer(null, _emptySettingFile), "foo");

            config.Initialize();

            Assert.Equal(new PathSetting("Databases/foo").FullPath, config.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("Databases/foo/Indexes").FullPath, config.Indexing.StoragePath.FullPath);

            Assert.Null(config.Indexing.TempPath);

            Assert.Null(config.Storage.TempPath);
        }

        [Fact]
        public void Inherits_server_settings_and_appends_resource_specific_suffix_paths()
        {
            var server = RavenConfiguration.CreateForServer(null);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}Deployment");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}temp");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}indexes-temp");

            server.Initialize();

            var database = RavenConfiguration.CreateForDatabase(server, "Foo");

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}Deployment\Databases\Foo").FullPath, database.Core.DataDirectory.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}temp\Databases\Foo").FullPath, database.Storage.TempPath.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}indexes-temp\Databases\Foo").FullPath, database.Indexing.TempPath.FullPath);
        }

        [Fact]
        public void Resource_specific_paths_do_not_require_any_suffixes()
        {
            var server = RavenConfiguration.CreateForServer(null);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}Deployment");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}temp");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}indexes-temp");

            server.Initialize();

            var database = RavenConfiguration.CreateForDatabase(server, "Foo");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}MyDatabase");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}my-temp-path");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}my-indexes-temp");

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}MyDatabase").FullPath, database.Core.DataDirectory.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}my-temp-path").FullPath, database.Storage.TempPath.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}my-indexes-temp").FullPath, database.Indexing.TempPath.FullPath);
        }

       [Fact]
        public void Should_create_data_in_directory_specified_at_server_level()
        {
            var server = RavenConfiguration.CreateForServer(null);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}RavenData");

            server.Initialize();

            var database = RavenConfiguration.CreateForDatabase(server, "Foo");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), @"Items");

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}RavenData\Items").FullPath, database.Core.DataDirectory.FullPath);
        }

        [Fact]
        public void Should_trim_last_directory_separator_character()
        {
            Assert.False(new PathSetting("Items\\").FullPath.EndsWith(@"\\"));
            Assert.False(new PathSetting("Items/").FullPath.EndsWith(@"/"));
        }

        public override void Dispose()
        {
            if (Directory.Exists(_emptySettingsDir))
                IOExtensions.DeleteDirectory(_emptySettingsDir);

            base.Dispose();
        }
    }
}
