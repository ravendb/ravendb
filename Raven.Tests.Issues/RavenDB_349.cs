using System.IO;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_349 : RavenTest
	{
		 [Fact]
		 public void CanUseUpdateAttachmentMetadata()
		 {
			 using(var store = NewDocumentStore())
			 {
				 store.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[]{1,2,3}), new RavenJObject
				 {
					 {"test", "yes"}
				 });

				 store.DatabaseCommands.UpdateAttachmentMetadata("test", null, new RavenJObject
				 {
					 {"test", "no"}
				 });

				 var attachment = store.DatabaseCommands.GetAttachment("test");

				 Assert.Equal(new byte[]{1,2,3}, attachment.Data().ReadData());
				 Assert.Equal("no", attachment.Metadata["test"]);
			 }
		 }
	}
}