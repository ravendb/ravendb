using Raven.Client.Indexes;
using Raven.Tests.Common;

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
				store.DocumentDatabase.Indexes.ResetIndex(ravenDocumentsByEntityName.IndexName);
			}
		}
	}
}
