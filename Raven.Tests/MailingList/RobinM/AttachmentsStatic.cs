using System;
using System.IO;
using System.Net;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList.RobinM
{
	public class AttachmentsStatic : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;
		private readonly string url;

		public AttachmentsStatic()
		{
			port = 8079;
			path = GetPath("TestDb");
			url = "http://localhost:" + port;
		}

		public new void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
		}

		[Fact]
		public void should_not_contain_exception_for_empty_attachment_on_instance_in_memory()
		{
			ShouldNotContainExceptionForEmptyAttachment(true);
		}

		[Fact]
		public void should_not_contain_exception_for_empty_attachment_on_instance_not_in_memory()
		{
			ShouldNotContainExceptionForEmptyAttachment(false);
		}

		public void ShouldNotContainExceptionForEmptyAttachment(bool runInMemory)
		{
			using (var server = GetNewServer(port, path, runInMemory))
			{
				using (var documentStore = new DocumentStore { Url = url })
				{
					documentStore.Initialize();

					using (Stream data = new MemoryStream(new byte[] { 1, 2, 3 }))
					{
						documentStore.DatabaseCommands
							.PutAttachment("attachment/1", null, data,
										   new RavenJObject
									   {
										   {"Description", "Random bytes"}
									   });

						documentStore.DatabaseCommands
							.PutAttachment("attachment/2", null, data,
										   new RavenJObject
											   {
												   {"Description", "Random bytes"}
											   });
					}


					Assert.DoesNotThrow(
						() =>
							{
								using (var wc = new WebClient())
								{
									wc.DownloadString(url + "/static");
								}
							});
				}
			}
		}
	}
}