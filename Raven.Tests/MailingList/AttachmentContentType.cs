// -----------------------------------------------------------------------
//  <copyright file="AttachmentContentType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class AttachmentContentType : RavenTest
	{
		[Fact]
		public void ShouldNotBeFiltered()
		{
			using(var store = NewDocumentStore())
			{
				using (Stream s = new MemoryStream())
				{
					store.DatabaseCommands.PutAttachment("images/image.jpg", null, s,
					                                     new RavenJObject {{"Content-Type", "image/jpeg"}});
				}

				var attachment = store.DatabaseCommands.GetAttachment("images/image.jpg");
				Assert.Equal("image/jpeg", attachment.Metadata["Content-Type"]);
			}
		}

		[Fact]
		public void CanGetOverHttp()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (Stream s = new MemoryStream())
				{
					store.DatabaseCommands.PutAttachment("images/image.jpg", null, s,
														 new RavenJObject { { "Content-Type", "image/jpeg" } });
				}

				var attachment = store.DatabaseCommands.GetAttachment("images/image.jpg");
				Assert.Equal("image/jpeg", attachment.Metadata["Content-Type"]);
			}
		}
	}
}