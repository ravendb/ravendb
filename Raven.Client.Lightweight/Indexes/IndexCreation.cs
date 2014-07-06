//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Exceptions;
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
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					task.Execute(databaseCommands, conventions);
				}
				catch (IndexCompilationException e)
				{
					indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
				}

			}

		    foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(databaseCommands, conventions);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.",indexCompilationExceptions);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					task.Execute(documentStore);
				}
				catch (IndexCompilationException e)
				{
					indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
				}
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
					task.Execute(documentStore);
			}

			if(indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
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
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
		    {
			    try
			    {
				    await task.ExecuteAsync(asyncDatabaseCommands, conventions);
			    }
				catch (IndexCompilationException e)
				{
					indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
				}
		    }
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(asyncDatabaseCommands, conventions);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}
	}
}
#endif
