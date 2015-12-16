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
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4066 : RavenTest
    {
        [Fact]
        public void database_directory_should_contain_database_resource_file_marker()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                Assert.True(File.Exists(Path.Combine(store.DataDirectory, Constants.Database.DbResourceMarker)));
            }
        }

        [Fact]
        public void should_throw_if_database_directory_contains_file_marker_of_different_kind_resource()
        {
            string resourceMarker = null;
            string path = null;

            using (var store = NewDocumentStore(runInMemory: false))
            {
                resourceMarker = Path.Combine(store.DataDirectory, Constants.Database.DbResourceMarker);
                path = store.DataDirectory;
            }

            File.Move(resourceMarker, resourceMarker.Replace(Constants.Database.DbResourceMarker, Constants.FileSystem.FsResourceMarker));

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new DocumentDatabase(new InMemoryRavenConfiguration()
                {
                    RunInMemory = false,
                    DataDirectory = path
                }))
                {

                }
            });

            Assert.Equal("The database data directory contains data of a different resource kind: file-system", exception.Message);
        }
    }
}