using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_505 : RavenTest
	{
		[Fact]
		public void CreateDeleteCreateIndex()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				var indexDefinition = new IndexDefinition
				{
					Map = "from d in docs select new {}"
				};
			    for (int i = 0; i < 10; i++)
			    {
                    store.DatabaseCommands.PutIndex("test", indexDefinition);
                    store.DatabaseCommands.DeleteIndex("test");
			    }
			}
		}


	}
}