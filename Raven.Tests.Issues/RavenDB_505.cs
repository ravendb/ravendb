using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_505 : RavenTest
	{
        [Theory]
        [PropertyData("Storages")]
		public void CreateDeleteCreateIndex(string storage)
		{
			using (var store = NewDocumentStore(requestedStorage: storage))
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