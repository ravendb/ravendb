using System.IO;
using System.Net;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList.RobinM
{
	public class AttachmentsStatic : RavenTest
	{
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
			using (var documentStore = NewRemoteDocumentStore(runInMemory: runInMemory))
			{
				documentStore.Initialize();

				using (Stream data = new MemoryStream(new byte[] {1, 2, 3}))
				{
					documentStore.DatabaseCommands.PutAttachment("attachment/1", null, data,
						new RavenJObject
						{
							{"Description", "Random bytes"}
						});

					documentStore.DatabaseCommands.PutAttachment("attachment/2", null, data,
						new RavenJObject
						{
							{"Description", "Random bytes"}
						});
				}


				Assert.DoesNotThrow(() =>
				{
					using (var wc = new WebClient())
					{
						wc.DownloadString("http://localhost:8079/static");
					}
				});
			}
		}
	}
}