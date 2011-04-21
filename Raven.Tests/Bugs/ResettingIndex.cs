using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ResettingIndex : LocalClientTest
	{
		[Fact]
		public void CanResetIndexInMuninStorage()
		{
			using (var store = NewDocumentStore("munin", false))
			{
				var ravenDocumentsByEntityName = new RavenDocumentsByEntityName();
				ravenDocumentsByEntityName.Execute(store);
				store.DocumentDatabase.ResetIndex(ravenDocumentsByEntityName.IndexName);
			}
		}

		[Fact]
		public void CanResetIndexInEsentStorage()
		{
			using (var store = NewDocumentStore("esent", false))
			{
				var ravenDocumentsByEntityName = new RavenDocumentsByEntityName();
				ravenDocumentsByEntityName.Execute(store);
				store.DocumentDatabase.ResetIndex(ravenDocumentsByEntityName.IndexName);
			}
		}
	}
}