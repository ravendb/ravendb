// -----------------------------------------------------------------------
//  <copyright file="RavenDB_384.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_384 : RavenTest
	{
		[Fact]
		public void Attachment_starts_with_remote()
		{
			using(GetNewServer())
			using (var x = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				x.DatabaseCommands.PutAttachment("test/1/a", null, new MemoryStream(), new RavenJObject());
				x.DatabaseCommands.PutAttachment("test/1/b", null, new MemoryStream(), new RavenJObject());

				var attachments = x.DatabaseCommands.GetAttachmentHeadersStartingWith("test/1", 0, 10).ToList();
				Assert.Equal(2, attachments.Count);
				Assert.Equal("test/1/a", attachments[0].Key);
				Assert.Equal("test/1/b", attachments[1].Key);
			}
		}
		[Fact]
		public void Attachment_starts_with_local()
		{
			using(var x = NewDocumentStore())
			{
				x.DatabaseCommands.PutAttachment("test/1/a", null, new MemoryStream(), new RavenJObject());
				x.DatabaseCommands.PutAttachment("test/1/b", null, new MemoryStream(), new RavenJObject());

				var attachments = x.DatabaseCommands.GetAttachmentHeadersStartingWith("test/1", 0, 10).ToList();
				Assert.Equal(2, attachments.Count);
				Assert.Equal("test/1/a", attachments[0].Key);
				Assert.Equal("test/1/b", attachments[1].Key);
			}
		}
	}
}