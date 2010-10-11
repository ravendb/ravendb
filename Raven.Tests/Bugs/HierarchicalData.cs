using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class HierarchicalData : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public HierarchicalData()
		{
			db =
				new DocumentDatabase(new RavenConfiguration
				{
					DataDirectory = "raven.db.test.esent",
				});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanCreateHierarchicalIndexes()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = @"
from post in docs.Posts
from comment in Hierarchy(post, ""Comments"") 
select new { comment.Text }"
			});

			db.Put("abc", null, JObject.Parse(@"
{
	'Name': 'Hello Raven',
	'Comments': [
		{ 'Author': 'Ayende', 'Text': 'def',	'Comments': [ { 'Author': 'Rahien', 'Text': 'abc' } ] }
	]
}
"), JObject.Parse("{'Raven-Entity-Name': 'Posts'}"), null);

			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("test", new IndexQuery
				{
					Query = "Text:abc"
				});
			} while (queryResult.IsStale);

			Assert.Equal(1, queryResult.Results.Count);
		}
	}
}
