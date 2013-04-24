//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !MONODROID && !NETFX_CORE
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Helper class for creating indexes from implementations of <see cref="AbstractIndexCreationTask"/>.
	/// </summary>
	public static class IndexCreation
	{
#if !SILVERLIGHT
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
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions)
		{
		    foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				task.Execute(databaseCommands, conventions);
			}

		    foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(databaseCommands, conventions);
            }
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
		    foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				task.Execute(documentStore);
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(documentStore);
            }
		}
#endif

		/// <summary>
		/// Creates the indexes found in the specified assembly.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static Task CreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			return CreateIndexesAsync(catalog, documentStore);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			return CreateIndexesAsync(catalogToGetnIndexingTasksFrom, documentStore.AsyncDatabaseCommands,
			                          documentStore.Conventions);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		public static async Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention conventions)
		{
		    foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
		    {
		        await task.ExecuteAsync(asyncDatabaseCommands, conventions);
		    }
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(asyncDatabaseCommands, conventions);
            }
		}
	}
}
#endif
