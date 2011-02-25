using Raven.Database.Indexing;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Create an index that aggregate all the document collections in the database
	/// </summary>
	public class RavenCollections : AbstractIndexCreationTask
	{
		/// <summary>
		/// Returns the index name
		/// </summary>
		public override string IndexName
		{
			get { return "Raven/DocumentCollections"; }
		}


		/// <summary>
		/// Returns the index definition
		/// </summary>
		public override IndexDefinition CreateIndexDefinition()
		{
			return new IndexDefinition
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
			};
		}
	}
}