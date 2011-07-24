using System;
using Raven.Abstractions.Indexing;
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
select new { Tag, LastModified = (DateTime)doc[""@metadata""][""Last-Modified""] };",
					Indexes =
					{
						{"Tag", FieldIndexing.NotAnalyzed},
					},
					Stores =
					{
						{"Tag", FieldStorage.No},
						{"LastModified", FieldStorage.No}
					}
				});
			}
		}
	}
}