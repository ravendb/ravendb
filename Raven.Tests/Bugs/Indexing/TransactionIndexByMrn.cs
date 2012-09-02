using System.ComponentModel.Composition.Hosting;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class TransactionIndexByMrn : RavenTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using (var store = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(Transaction_ByMrn))), store);
			}
		}
	}
}
