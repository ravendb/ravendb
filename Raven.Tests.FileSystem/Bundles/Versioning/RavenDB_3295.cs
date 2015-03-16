// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3295.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Bundles.Versioning
{
	public class RavenDB_3295 : RavenFilesTestBase
	{
		[Fact]
		public async Task ShouldCreateRevisionsOnlyForSpecificDir()
		{
			using (var store = NewStore(activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync("Raven/Versioning/documents/images", new FileVersioningConfiguration { Id = "Raven/Versioning/documents/images", MaxRevisions = 10 });

				await store.AsyncFilesCommands.Configuration.SetKeyAsync("Raven/Versioning/documents/images/photos", new FileVersioningConfiguration { Id = "Raven/Versioning/documents/images/photos", Exclude = true });

				using (var session = store.OpenAsyncSession())
				{
					session.RegisterUpload("/documents/images/versioned.jpg", CreateRandomFileStream(10));
					session.RegisterUpload("/documents/unversioned.jpg", CreateRandomFileStream(10));
					session.RegisterUpload("/documents/images/photos/unversioned.jpg", CreateRandomFileStream(10));
					session.RegisterUpload("unversioned.jpg", CreateRandomFileStream(10));

					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var revisions = await session.GetRevisionsForAsync("/documents/images/versioned.jpg", 0, 10);
					Assert.Equal(1, revisions.Length);

					revisions = await session.GetRevisionsForAsync("/documents/unversioned.jpg", 0, 10);
					Assert.Equal(0, revisions.Length);

					revisions = await session.GetRevisionsForAsync("/documents/images/photos/unversioned.jpg", 0, 10);
					Assert.Equal(0, revisions.Length);

					revisions = await session.GetRevisionsForAsync("unversioned.jpg", 0, 10);
					Assert.Equal(0, revisions.Length);
				}
			}
		}

		[Fact]
		public async Task ShouldRespectConfigSpecificSettings()
		{
			using (var store = NewStore(activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 4 });

				await store.AsyncFilesCommands.Configuration.SetKeyAsync("Raven/Versioning/documents/images", new FileVersioningConfiguration { Id = "Raven/Versioning/documents/images", MaxRevisions = 2 });


				for (int i = 0; i < 5; i++)
				{
					using (var session = store.OpenAsyncSession())
					{
						session.RegisterUpload("/documents/images/versioned.jpg", CreateRandomFileStream(10));
						session.RegisterUpload("versioned.jpg", CreateRandomFileStream(10));

						await session.SaveChangesAsync();
					}
				}

				using (var session = store.OpenAsyncSession())
				{
					var revisions = await session.GetRevisionsForAsync("/documents/images/versioned.jpg", 0, 10);
					Assert.Equal(2, revisions.Length);

					revisions = await session.GetRevisionsForAsync("versioned.jpg", 0, 10);
					Assert.Equal(4, revisions.Length);
				}
			}
		}
	}
}