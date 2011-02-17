namespace Raven.Studio.Features.StartUp
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Threading.Tasks;
	using Client;
	using Raven.Database.Indexing;

	[Export(typeof (IDatabaseInitializer))]
	public class DefaultDatabaseInitializer : IDatabaseInitializer
	{
		public IEnumerable<Task> Initialize(IAsyncDocumentSession session)
		{
			yield return session.Advanced.AsyncDatabaseCommands
				.PutIndexAsync(@"Studio/DocumentCollections",
				               new IndexDefinition
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
				               	}, true);
		}
	}
}