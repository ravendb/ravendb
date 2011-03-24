using System;
using Raven.Database.Indexing;
using Raven.Http;
using Raven.Http.Plugins;

namespace Raven.Database.Plugins.Builtins
{
	public class CreateSilverlightIndexes : ISilverlightRequestedAware
	{
		public void SilverlightWasRequested(IResourceStore resourceStore)
		{
			var documentDatabase = ((DocumentDatabase)resourceStore);
			if (documentDatabase.GetIndexDefinition("Raven/DocumentsByEntityName") == null)
			{
				documentDatabase.PutIndex("Raven/DocumentsByEntityName", new IndexDefinition
				{
					Map =
						@"from doc in docs 
let Tag = doc[""@metadata""][""Raven-Entity-Name""]
where  Tag != null 
select new { Tag };",
					Indexes = { { "Tag", FieldIndexing.NotAnalyzed } },
					Stores = { { "Tag", FieldStorage.No } }
				});
			}

			if (documentDatabase.GetIndexDefinition("Raven/DocumentCollections") == null)
			{
				documentDatabase.PutIndex("Raven/DocumentCollections", new IndexDefinition
				{
					Map =
						@"from doc in docs
let Name = doc[""@metadata""][""Raven-Entity-Name""]
where Name != null
select new { Name , Count = 1}
",
					Reduce =
						@"from result in results
group result by result.Name into g
select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
				});
			}
		}
	}
}