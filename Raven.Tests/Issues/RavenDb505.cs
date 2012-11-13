using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDb505 : RavenTest
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
				store.DatabaseCommands.PutIndex("test", indexDefinition);
				store.DatabaseCommands.DeleteIndex("test");
				store.DatabaseCommands.PutIndex("test", indexDefinition);
			}
		}


	}
}