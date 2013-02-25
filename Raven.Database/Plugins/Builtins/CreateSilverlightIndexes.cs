using System;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;

namespace Raven.Database.Plugins.Builtins
{
	public class CreateSilverlightIndexes : ISilverlightRequestedAware
	{
		public void SilverlightWasRequested(DocumentDatabase database)
		{
			if (database.GetIndexDefinition("Raven/DocumentsByEntityName") == null)
			{
				database.PutIndex("Raven/DocumentsByEntityName", new IndexDefinition
				{
					Map =
						@"from doc in docs 
let Tag = doc[""@metadata""][""Raven-Entity-Name""]
select new { Tag, LastModified = (DateTime)doc[""@metadata""][""Last-Modified""] };",
					Indexes =
					{
						{"Tag", FieldIndexing.NotAnalyzed},
						{"LastModified", FieldIndexing.NotAnalyzed},
					},
					Stores =
					{
						{"Tag", FieldStorage.No},
						{"LastModified", FieldStorage.No}
					},
					TermVectors =
					{
						{"Tag", FieldTermVector.No},
						{"LastModified", FieldTermVector.No}						
					}
				});
			}
		}
	}
}