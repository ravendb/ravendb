using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ResettingIndex : RavenTest
	{
		[Fact]
		public void CanResetIndex()
		{
			using (var store = NewDocumentStore())
			{
				var ravenDocumentsByEntityName = new RavenDocumentsByEntityName();
				ravenDocumentsByEntityName.Execute(store);
				store.DocumentDatabase.ResetIndex(ravenDocumentsByEntityName.IndexName);
			}
		}
	}
}
