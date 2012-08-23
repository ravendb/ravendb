using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Attachments : RavenTest
	{
		[Fact]
		public void CanExportAttachments()
		{
			using (var server = GetNewServer())
			{
				using (var documentStore = new DocumentStore { Url = server.Database.Configuration.ServerUrl }.Initialize())
				{
					documentStore.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test2", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 5 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test3", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test4", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 5 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test5", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test6", null, new MemoryStream(new byte[] { 1, 2, 3, 5 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test7", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());
					documentStore.DatabaseCommands.PutAttachment("test8", Guid.NewGuid(), new MemoryStream(new byte[] { 1, 2, 3, 5 }), new RavenJObject());
				}

				using (var webClient = new WebClient())
				{
					webClient.UseDefaultCredentials = true;
					webClient.Credentials = CredentialCache.DefaultNetworkCredentials;

					var lastEtag = Guid.Empty;
					int totalCount = 0;
					while (true)
					{
						var attachmentInfo =
							GetString(webClient.DownloadData(server.Database.Configuration.ServerUrl + "/static/?pageSize=2&etag=" + lastEtag));
						var array = RavenJArray.Parse(attachmentInfo);

						if (array.Length == 0) break;

						totalCount += array.Length;

						lastEtag = new Guid(array.Last().Value<string>("Etag"));
					}
				}
			}
		}

		public static string GetString(byte[] downloadData)
		{
			var ms = new MemoryStream(downloadData);
			return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
		}
	}
}
