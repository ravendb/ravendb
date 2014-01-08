// -----------------------------------------------------------------------
//  <copyright file="AttachmentContentType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class AttachmentContentType : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.DefaultStorageTypeName = "esent";
		}

		[Fact]
		public void ShouldNotBeFiltered()
		{
			using(var store = NewDocumentStore())
			{
				var dummyImageBytes = new Byte[] { 1,2,3,4 }; //creating empty attachment will fail
				using (var dummyImageData = new MemoryStream(dummyImageBytes))
				{
					store.DatabaseCommands.PutAttachment("images/image.jpg", null, dummyImageData,
					                                     new RavenJObject {{"Content-Type", "image/jpeg"}});
				}

				var attachment = store.DatabaseCommands.GetAttachment("images/image.jpg");
				Assert.Equal("image/jpeg", attachment.Metadata["Content-Type"]);
			}
		}

		[Fact]
		public void Attachment_metadata_stored_and_fetched_correctly()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage:"esent"))
			{
				var dummyImageBytes = new Byte[] { 1, 2, 3, 4 }; //creating empty attachment will fail
				using (var dummyImageData = new MemoryStream(dummyImageBytes))
				{
					store.DatabaseCommands.PutAttachment("images/image.jpg", null, dummyImageData,
														 new RavenJObject { { "Attachment-file-type", "image/jpeg" } });
				}

				var attachment = store.DatabaseCommands.GetAttachment("images/image.jpg");
				Assert.Equal("image/jpeg", attachment.Metadata["Attachment-file-type"]);
			}
		}

		[Fact]
		public void Attachment_metadata_content_type_should_overwrite_http_content_type()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
				var dummyImageBytes = new Byte[] { 1, 2, 3, 4 }; //creating empty attachment will fail
				using (var dummyImageData = new MemoryStream(dummyImageBytes))
				{
					store.DatabaseCommands.PutAttachment("images/image.jpg", null, dummyImageData,
														 new RavenJObject { { "Content-Type", "image/jpeg" },{"Foo", "Bar"} });
				}

				var attachment = store.DatabaseCommands.GetAttachment("images/image.jpg");
				Assert.Equal("image/jpeg", attachment.Metadata["Content-Type"]);
			}
		}
	
	}
}