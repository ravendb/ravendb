// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3979.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.Config;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Database.FileSystem.Bundles.Versioning.Plugins;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.FileSystem.Bundles.Versioning
{
    public class RavenDB_3979_files : RavenFilesTestBase
    {
        [Fact]
        public async Task must_not_allow_to_create_historical_file_if_changes_to_revisions_are_not_allowed()
        {
            using (var store = NewStore(activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 10 });

                var exception = await AssertAsync.Throws<ErrorResponseException>(() => store.AsyncFilesCommands.UploadAsync("files/1/revision", new MemoryStream(), new RavenJObject() { { VersioningUtil.RavenFileRevisionStatus, "Historical" } }));

                Assert.Contains(VersioningTriggerActions.CreationOfHistoricalRevisionIsNotAllowed, exception.Message);
            }
        }

        [Fact]
        public async Task allows_to_create_historical_file_if_changes_to_revisions_are_allowed()
        {
            using (var store = NewStore(activeBundles: "Versioning", customConfig: configuration => configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed] = "true"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 10 });


                Assert.True(await AssertAsync.DoesNotThrow(() => store.AsyncFilesCommands.UploadAsync("files/1/revision", new MemoryStream(), new RavenJObject() { { VersioningUtil.RavenFileRevisionStatus, "Historical" } })));
            }
        }
    }
}