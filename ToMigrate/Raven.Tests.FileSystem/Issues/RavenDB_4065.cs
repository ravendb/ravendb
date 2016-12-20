// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4065.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Database.Config;
using Raven.Database.FileSystem;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4065 : RavenFilesTestBase
    {
        [Fact]
        public void file_system_configured_to_use_voron_cannot_point_to_esent_data()
        {
            string path = null;

            using (NewStore(requestedStorage: "esent", runInMemory: false))
            {
                path = GetFileSystem(0).Configuration.FileSystem.DataDirectory;
            }

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new RavenFileSystem(new InMemoryRavenConfiguration() { FileSystem = { DefaultStorageTypeName = "voron", DataDirectory = path } }, null))
                {
                    
                }
            });

            Assert.Equal("The file system is configured to use 'voron' storage engine, but it points to 'esent' data", exception.Message);
        }

        [Fact]
        public void file_system_configured_to_use_esent_cannot_point_to_voron_data()
        {
            string path = null;

            using (NewStore(requestedStorage: "voron", runInMemory: false))
            {
                path = GetFileSystem(0).Configuration.FileSystem.DataDirectory;
            }

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new RavenFileSystem(new InMemoryRavenConfiguration() { FileSystem = { DefaultStorageTypeName = "esent", DataDirectory = path } }, null))
                {

                }
            });

            Assert.Equal("The file system is configured to use 'esent' storage engine, but it points to 'voron' data", exception.Message);
        }
    }
}