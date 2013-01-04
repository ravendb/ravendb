using System;
using Raven.Abstractions.Indexing;

namespace Raven.Client.Indexes
{
	///<summary>
	/// Create an index that allows to tag entities by their entity name
	///</summary>
	public class RavenDocumentsByEntityName : AbstractIndexCreationTask
	{
		/// <summary>
		/// Return the actual index name
		/// </summary>
		public override string IndexName
		{
			get { return "Raven/DocumentsByEntityName"; }
		}
		
		/// <summary>
		/// Creates the Raven/DocumentsByEntityName index
		/// </summary>
		public override IndexDefinition CreateIndexDefinition()
		{
			return new IndexDefinition
			{
				Map = @"from doc in docs 
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
					}
			};
		}
	}
}
