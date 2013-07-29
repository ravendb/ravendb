//-----------------------------------------------------------------------
// <copyright file="AttachmentEncoding.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using System.Net;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AttachmentEncoding : RemoteClientTest
	{
		[Fact]
		public void Can_get_proper_attachment_names()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.PutAttachment("test/hello/world", null, new MemoryStream(new byte[] {1, 2, 3}),
					new RavenJObject());

				using (var wc = new WebClient())
				{
					var staticJson = wc.DownloadString("http://localhost:8079/static");
					var value = RavenJArray.Parse(staticJson)[0].Value<string>("Key");
					Assert.Equal("test/hello/world", value);
				}
			}
		}
	}
}