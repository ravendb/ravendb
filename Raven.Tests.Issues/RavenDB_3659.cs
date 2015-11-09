// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3659.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3659 : RavenTest
    {
        [Fact]
        public void IfTempPathCannotBeAccessedThenServerShouldThrowDuringStartup()
        {
            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                using (var store = new EmbeddableDocumentStore { RunInMemory = true, Configuration = { Core = { TempPath = "ZF1:\\" } } }.Initialize())
                {
                }
            });

            Assert.Equal("Could not access temp path 'ZF1:\\'. Please check if you have sufficient privileges to access this path or change 'Raven/TempPath' value.", e.Message);
        }

        [Fact]
        public void TenantDatabasesShouldInheritTempPathIfNoneSpecified()
        {
            var path = NewDataPath();

            using (var store = new EmbeddableDocumentStore { RunInMemory = true, DefaultDatabase = "DB1", Configuration = { Core = { TempPath = path } } })
            {
                store.Initialize();

                Assert.Equal(path, store.SystemDatabase.Configuration.Core.TempPath);

                Assert.Equal("DB1", store.DocumentDatabase.Name);
                Assert.Equal(path, store.DocumentDatabase.Configuration.Core.TempPath);
            }
        }

        [Fact]
        public void TenantDatabasesCanHaveDifferentTempPathSpecified()
        {
            var path1 = NewDataPath();
            var path2 = NewDataPath();

            Assert.NotEqual(path1, path2);

            using (var store = new EmbeddableDocumentStore { RunInMemory = true, Configuration = { Core = { TempPath = path1 }}})
            {
                store.Initialize();

                Assert.Equal(path1, store.SystemDatabase.Configuration.Core.TempPath);

                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "DB1",
                        Settings =
                        {
                            { "Raven/DataDir", NewDataPath() },
                            { RavenConfiguration.GetKey(x => x.Core.TempPath), path2 }
                        }
                    });

                var database = store.ServerIfEmbedded.Options.DatabaseLandlord.GetResourceInternal("DB1").Result;
                Assert.Equal("DB1", database.Name);
                Assert.Equal(path2, database.Configuration.Core.TempPath);
            }
        }
    }
}
