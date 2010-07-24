using System.ComponentModel.Composition.Hosting;
using System.Reflection;

namespace Raven.Client.Indexes
{
	public static class IndexCreation
	{
		public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			CreateIndexes(catalog, documentStore);
		}

		public static void CreateIndexes(CompositionContainer catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
			foreach (var task in tasks)
			{
				task.Execute(documentStore);
			}
		}
	}
}