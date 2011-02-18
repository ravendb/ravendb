namespace Raven.Studio.Features.StartUp
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client;
	using Collections;
	using Documents;
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


			// preload collection templates
			var templateProvider = IoC.Get<IDocumentTemplateProvider>();
			var collections = session.Advanced.AsyncDatabaseCommands.GetCollectionsAsync(0, 25);
			yield return collections;

			var preloading  = collections.Result
				.Select(x=>x.Name)
				.Union(BuiltinCollectionName.All<BuiltinCollectionName>().Select(x => x.Value))
				.Select(templateProvider.GetTemplateFor);

			foreach (var task in preloading)
				yield return task;
		}
	}
}