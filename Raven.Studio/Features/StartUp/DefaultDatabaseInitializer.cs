namespace Raven.Studio.Features.StartUp
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client;
	using Client.Extensions;
	using Client.Indexes;
	using Collections;
	using Documents;
	using Framework;

	[Export(typeof (IDatabaseInitializer))]
	public class DefaultDatabaseInitializer : IDatabaseInitializer
	{
		public IEnumerable<Task> Initialize(IAsyncDocumentSession session)
		{
			yield return session.Advanced.AsyncDatabaseCommands
				.PutIndexAsync<RavenDocumentsByEntityName>(true);			
				
			yield return session.Advanced.AsyncDatabaseCommands
				.PutIndexAsync<RavenCollections>(true);

			// preload collection templates
			var templateProvider = IoC.Get<IDocumentTemplateProvider>();
			var collections = session.Advanced.AsyncDatabaseCommands.GetCollectionsAsync(0, 25);
			yield return collections;

			var preloading  = collections.Result
				.Select(x=>x.Name)
				.Union(Enumeration.All<BuiltinCollectionName>().Select(x => x.Value))
				.Select(templateProvider.GetTemplateFor);

			foreach (var task in preloading)
				yield return task;
		}
	}
}