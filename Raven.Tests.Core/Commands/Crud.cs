// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class Crud : RavenCoreTestBase
	{
		[Fact]
		public async Task CanPutAndDeleteDocument()
		{
			using (var store = GetDocumentStore())
			{
				var putResult = await store.AsyncDatabaseCommands.PutAsync("items/1", null, new RavenJObject() { { "Key", "Value" } }, new RavenJObject());

				Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("items/1"));

				await store.AsyncDatabaseCommands.DeleteAsync("items/1", putResult.ETag);

				Assert.Null(await store.AsyncDatabaseCommands.GetAsync("items/1"));
			}
		}

		[Fact]
		public async Task CanPutUpdateMetadataAndDeleteAttachment()
		{
			using (var store = GetDocumentStore())
			{
				await store.AsyncDatabaseCommands.PutAttachmentAsync("items/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

				Assert.NotNull(await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1"));

				await store.AsyncDatabaseCommands.UpdateAttachmentMetadataAsync("items/1", null, new RavenJObject() { { "attachment_key", "value" } });

				var attachment = await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1");

				Assert.Equal("value", attachment.Metadata.Value<string>("attachment_key"));

				await store.AsyncDatabaseCommands.DeleteAttachmentAsync("items/1", null);

				Assert.Null(await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1"));
			}
		}

		[Fact]
		public async Task CanPutUpdateAndDeleteMapIndex()
		{
			using (var store = GetDocumentStore())
			{
				const string usersByname = "users/byName";

				await store.AsyncDatabaseCommands.PutIndexAsync(usersByname, new IndexDefinition()
				{
					Map = "from user in docs.Users select new { user.Name }"
				}, false);

				var result = await store.AsyncDatabaseCommands.GetIndexAsync(usersByname);
				Assert.Equal(usersByname, result.Name);

				await store.AsyncDatabaseCommands.PutIndexAsync(usersByname, new IndexDefinition()
				{
					Map = "from user in docs.Users select new { user.FirstName, user.LastName }"
				}, true);

				var indexDefinition = await store.AsyncDatabaseCommands.GetIndexAsync(usersByname);

				Assert.Equal("from user in docs.Users select new { user.FirstName, user.LastName }", indexDefinition.Map);

				await store.AsyncDatabaseCommands.DeleteIndexAsync(usersByname);

				Assert.Null(await store.AsyncDatabaseCommands.GetIndexAsync(usersByname));
			}
		}

		[Fact]
		public async Task CanPutUpdateAndDeleteTransformer()
		{
			using (var store = GetDocumentStore())
			{
				const string usersSelectNames = "users/selectName";

				await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
				{
					Name = usersSelectNames,
					TransformResults = "from user in results select new { Name = user.Name }"
				});

				await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
				{
					Name = usersSelectNames,
					TransformResults = "from user in results select new { user.FirstName, user.LastName }"
				});

				var transformer = await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames);

				Assert.Equal("from user in results select new { user.FirstName, user.LastName }", transformer.TransformResults);

				await store.AsyncDatabaseCommands.DeleteTransformerAsync(usersSelectNames);

				Assert.Null(await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames));
			}
		}

	}
}