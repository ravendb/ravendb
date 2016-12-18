// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4066.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.FileSystem;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4066 : RavenFilesTestBase
    {
        [Fact]
        public void file_system_directory_should_contain_file_system_resource_file_marker()
        {
            using (var store = NewStore(runInMemory: false))
            {
                var fs = GetFileSystem(0);

                Assert.True(File.Exists(Path.Combine(fs.Configuration.FileSystem.DataDirectory, Constants.FileSystem.FsResourceMarker)));
            }
        }

        [Fact]
        public void should_throw_if_database_directory_contains_file_marker_of_different_kind_resource()
        {
            string resourceMarker = null;
            string path = null;

            using (var store = NewStore(runInMemory: false))
            {
                var fs = GetFileSystem(0);

                resourceMarker = Path.Combine(fs.Configuration.FileSystem.DataDirectory, Constants.FileSystem.FsResourceMarker);
                path = fs.Configuration.FileSystem.DataDirectory;
            }

            File.Move(resourceMarker, resourceMarker.Replace(Constants.FileSystem.FsResourceMarker, Constants.Database.DbResourceMarker));

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new RavenFileSystem(new InMemoryRavenConfiguration()
                {
                    RunInMemory = false,
                    FileSystem =
                    {
                        DataDirectory = path
                    }
                }, null))
                {

                }
            });

            Assert.Equal("The file system data directory contains data of a different resource kind: database", exception.Message);
        }
    }
}