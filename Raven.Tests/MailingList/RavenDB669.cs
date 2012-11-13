using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenDB669 : RavenTest
	{
		[Fact]
		public void GenerateIdentityLocal()
		{
			using (var store = NewDocumentStore())
			{
				Assert.Equal(1, store.DatabaseCommands.NextIdentityFor("chairs"));
				Assert.Equal(2, store.DatabaseCommands.NextIdentityFor("chairs"));
			}
		}

		[Fact]
		public void GenerateIdentityRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				Assert.Equal(1, store.DatabaseCommands.NextIdentityFor("chairs"));
				Assert.Equal(2, store.DatabaseCommands.NextIdentityFor("chairs"));
			}
		}
	}
}