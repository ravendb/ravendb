using System.Net;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Document
{
	public class DocumentStoreServerGranularTests : NoDisposalNeeded
	{
		[Fact]
		public void Can_read_credentials_from_connection_string()
		{
			using (var documentStore = new DocumentStore { ConnectionStringName = "Secure" })
			{
				Assert.NotNull(documentStore.Credentials);
				var networkCredential = (NetworkCredential)documentStore.Credentials;
				Assert.Equal("beam", networkCredential.UserName);
				Assert.Equal("up", networkCredential.Password);
			}
		} 
	}
}