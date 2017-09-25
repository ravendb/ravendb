using System;
using System.IO;
using FastTests.Utils;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6141 : IDisposable
    {
        private readonly string _rootPathString = LinuxTestUtils.RunningOnPosix ? @"/" : @"C:\";

        private readonly string _emptySettingFile;

        public RavenDB_6141()
        {
            _emptySettingFile = Path.Combine(Path.GetTempPath(), "settings.json");
            using (var f = File.CreateText(_emptySettingFile))
                f.Write("{}");
        }

        [Fact]
        public void Default_database_path_settings()
        {
            var config = new RavenConfiguration("foo", ResourceType.Database, _emptySettingFile);

            config.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            config.Initialize();

            Assert.Equal(new PathSetting("Databases/foo").FullPath, config.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("Databases/foo/Indexes").FullPath, config.Indexing.StoragePath.FullPath);

            Assert.Null(config.Indexing.TempPath);
            Assert.Null(config.Indexing.JournalsStoragePath);

            Assert.Null(config.Storage.TempPath);
            Assert.Null(config.Storage.JournalsStoragePath);

            // actual configuration is created in the following manner

            config = RavenConfiguration.CreateFrom(new RavenConfiguration(null, ResourceType.Server), "foo",
                ResourceType.Database);

            config.Initialize();

            Assert.Equal(new PathSetting("Databases/foo").FullPath, config.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting("Databases/foo/Indexes").FullPath, config.Indexing.StoragePath.FullPath);

            Assert.Null(config.Indexing.TempPath);
            Assert.Null(config.Indexing.JournalsStoragePath);

            Assert.Null(config.Storage.TempPath);
            Assert.Null(config.Storage.JournalsStoragePath);
        }

        [Fact]
        public void Inherits_server_settings_and_appends_resource_specific_suffix_paths()
        {
            var server = new RavenConfiguration(null, ResourceType.Server);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}Deployment");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}temp");
            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), $@"{_rootPathString}Journals");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}indexes-temp");
            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.JournalsStoragePath), $@"{_rootPathString}Indexes-Journals");

            server.Initialize();

            var database = RavenConfiguration.CreateFrom(server, "Foo", ResourceType.Database);

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}Deployment\Databases\Foo").FullPath, database.Core.DataDirectory.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}temp\Databases\Foo").FullPath, database.Storage.TempPath.FullPath);
            Assert.Equal(new PathSetting($@"{_rootPathString}Journals\Databases\Foo").FullPath, database.Storage.JournalsStoragePath.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}indexes-temp\Databases\Foo").FullPath, database.Indexing.TempPath.FullPath);
            Assert.Equal(new PathSetting($@"{_rootPathString}Indexes-Journals\Databases\Foo").FullPath, database.Indexing.JournalsStoragePath.FullPath);
        }

        [Fact]
        public void Resource_specific_paths_do_not_require_any_suffixes()
        {
            var server = new RavenConfiguration(null, ResourceType.Server);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}Deployment");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}temp");
            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), $@"{_rootPathString}Journals");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}indexes-temp");
            server.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.JournalsStoragePath), $@"{_rootPathString}Indexes-Journals");

            server.Initialize();

            var database = RavenConfiguration.CreateFrom(server, "Foo", ResourceType.Database);

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}MyDatabase");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Storage.TempPath), $@"{_rootPathString}my-temp-path");
            database.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), $@"{_rootPathString}MyJournals");

            database.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.TempPath), $@"{_rootPathString}my-indexes-temp");
            database.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.JournalsStoragePath), $@"{_rootPathString}My-Indexes-Journals");

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}MyDatabase").FullPath, database.Core.DataDirectory.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}my-temp-path").FullPath, database.Storage.TempPath.FullPath);
            Assert.Equal(new PathSetting($@"{_rootPathString}MyJournals").FullPath, database.Storage.JournalsStoragePath.FullPath);

            Assert.Equal(new PathSetting($@"{_rootPathString}my-indexes-temp").FullPath, database.Indexing.TempPath.FullPath);
            Assert.Equal(new PathSetting($@"{_rootPathString}My-Indexes-Journals").FullPath, database.Indexing.JournalsStoragePath.FullPath);
        }

        [Fact]
        public void Should_handle_APPDRIVE_properly_if_specified()
        {
            var server = new RavenConfiguration(null, ResourceType.Server);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), @"APPDRIVE:\RavenData");

            server.Initialize();

            var rootPath = Path.GetPathRoot(AppContext.BaseDirectory);

            Assert.Equal(new PathSetting($@"{rootPath}RavenData").FullPath, server.Core.DataDirectory.FullPath);

            var database = RavenConfiguration.CreateFrom(server, "Foo", ResourceType.Database);

            database.Initialize();

            Assert.Equal(new PathSetting($@"{rootPath}RavenData\Databases\Foo").FullPath, database.Core.DataDirectory.FullPath);
            Assert.Equal(new PathSetting($@"{rootPath}RavenData\Databases\Foo\Indexes").FullPath, database.Indexing.StoragePath.FullPath);
        }

        [Fact]
        public void Should_create_data_in_directory_specified_at_server_level()
        {
            var server = new RavenConfiguration(null, ResourceType.Server);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.RunInMemory), "true");

            server.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), $@"{_rootPathString}RavenData");

            server.Initialize();

            var database = RavenConfiguration.CreateFrom(server, "Foo", ResourceType.Database);

            database.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), @"~/Items");

            database.Initialize();

            Assert.Equal(new PathSetting($@"{_rootPathString}RavenData\Items").FullPath, database.Core.DataDirectory.FullPath);
        }

        [Fact]
        public void Should_trim_last_directory_separator_character()
        {
            Assert.False(new PathSetting("~\\Items\\").FullPath.EndsWith(@"\\"));
            Assert.False(new PathSetting("~/Items/").FullPath.EndsWith(@"/"));
        }

        public void Dispose()
        {
            if (_emptySettingFile != null)
                IOExtensions.DeleteFile(_emptySettingFile);
        }
    }
}
