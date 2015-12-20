// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4065.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4065 : RavenTest
    {
        [Fact]
        public void database_configured_to_use_voron_cannot_point_to_esent_data()
        {
            string path = null;

            using (var store = NewDocumentStore(requestedStorage: "esent", runInMemory: false))
            {
                path = store.DocumentDatabase.Configuration.DataDirectory;
            }

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new DocumentDatabase(new InMemoryRavenConfiguration()
                                            {
                                                DefaultStorageTypeName = "voron",
                                                DataDirectory = path
                                            }))
                {

                }
            });

            Assert.Equal("The database is configured to use 'voron' storage engine, but it points to 'esent' data", exception.Message);
        }

        [Fact]
        public void database_configured_to_use_esent_cannot_point_to_voron_data()
        {
            string path = null;

            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                path = store.DocumentDatabase.Configuration.DataDirectory;
            }

            var exception = Assert.Throws<Exception>(() =>
            {
                using (new DocumentDatabase(new InMemoryRavenConfiguration()
                {
                    DefaultStorageTypeName = "esent",
                    DataDirectory = path
                }))
                {

                }
            });

            Assert.Equal("The database is configured to use 'esent' storage engine, but it points to 'voron' data", exception.Message);
        }
    }
}