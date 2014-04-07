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
using Raven.Tests.Core.Utils.Entities;
using Raven.Abstractions.Data;
using System.Collections.Generic;

namespace Raven.Tests.Core.Commands
{
	public class Crud : RavenCoreTestBase
	{
		[Fact]
		public async Task CanPutUpdateAndDeleteDocument()
		{
			using (var store = GetDocumentStore())
			{
                var putResult = await store.AsyncDatabaseCommands.PutAsync(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company
                        {
                            Name = "testname",
                            Phone = 1,
                            Contacts = new List<Contact> { new Contact{}, new Contact{} },
                            Address1 = "To be removed.",
                            Address2 = "Address2"
                        }),
                    new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.PutAsync("users/2", null, RavenJObject.FromObject(new User { Name = "testname2" }), new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("users/2"));

                var documents = await store.AsyncDatabaseCommands.GetDocumentsAsync(0, 25);
                Assert.Equal(2, documents.Length);

                await store.AsyncDatabaseCommands.PatchAsync(
                    "companies/1", 
                    new[]
                        {
                            new PatchRequest 
                                {
                                    Type = PatchCommandType.Add,
                                    Name = "NewArray",
                                    Value = "NewValue"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Copy,
                                    Name = "Name",
                                    Value = "CopiedName"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Inc,
                                    Name = "Phone",
                                    Value = -1
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Insert,
                                    Name = "Contacts",
                                    Position = 1,
                                    Value = RavenJObject.FromObject( new Contact { FirstName = "TestFirstName" } )
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Modify,
                                    Name = "Contacts",
                                    Position = 0,
                                    Nested = new[]
                                    {
                                        new PatchRequest
                                        {
                                            Type = PatchCommandType.Set,
                                            Name = "FirstName",
                                            Value = "SomeFirstName"
                                        }
                                    }
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Rename,
                                    Name = "Address2",
                                    Value = "Renamed"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Unset,
                                    Name = "Address1"
                                }
                        },
                    null);

                var item1 = await store.AsyncDatabaseCommands.GetAsync("companies/1");
                Assert.NotNull(item1);
                Assert.Equal("NewValue", item1.DataAsJson.Value<RavenJArray>("NewArray")[0]);
                Assert.Equal("testname", item1.DataAsJson.Value<string>("CopiedName"));
                Assert.Equal(0, item1.DataAsJson.Value<int>("Phone"));
                Assert.Equal("TestFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[1].Value<string>("FirstName"));
                Assert.Equal("SomeFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[0].Value<string>("FirstName"));
                Assert.Null(item1.DataAsJson.Value<string>("Address1"));
                Assert.Null(item1.DataAsJson.Value<string>("Address2"));
                Assert.Equal("Address2", item1.DataAsJson.Value<string>("Renamed"));

                await store.AsyncDatabaseCommands.DeleteAsync("companies/1", putResult.ETag);
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.DeleteDocumentAsync("users/2");
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("users/2"));
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