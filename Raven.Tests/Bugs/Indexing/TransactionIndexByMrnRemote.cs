using System.ComponentModel.Composition.Hosting;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class TransactionIndexByMrnRemote : RavenTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(Transaction_ByMrn))), store);
			}
		}
	}
}