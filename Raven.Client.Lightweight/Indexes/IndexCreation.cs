using System.ComponentModel.Composition.Hosting;
using System.Reflection;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Helper class for creating indexed from implementations of <see cref="AbstractIndexCreationTask"/>.
	/// </summary>
	public static class IndexCreation
	{
		/// <summary>
		/// Creates the indexes found in the specified assembly.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			CreateIndexes(catalog, documentStore);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to getn indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
			foreach (var task in tasks)
			{
				task.Execute(documentStore);
			}
		}
	}
}
