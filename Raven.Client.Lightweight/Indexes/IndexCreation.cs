//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
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
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			try
			{
				var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
				var indexesNames = tasks.Select(x => x.IndexName).ToList();
				// need to make sure we dont replace legacy indexes
				var serverDefinitions = databaseCommands.GetspecifiedIndexes(indexesNames.ToArray());
				List<IndexDefinition> definitions = new List<IndexDefinition>();
				List<string> names = new List<string>();
				int i = 0;
				foreach (var task in tasks)
				{
					var clientDefinitions = task.CreateIndexDefinition();
					if (serverDefinitions[i] == null || !task.CurrentOrLegacyIndexDefinitionEquals(conventions, serverDefinitions[i], clientDefinitions))
					{
						names.Add(task.IndexName);
						definitions.Add(clientDefinitions);
					}
					i++;
				}
				//Only executing missing/updated indexes
				databaseCommands.PutIndexes(names, definitions.ToArray());
			}
			// For old servers that don't have the new entrypoint for executing multiple indexes
			catch (Exception)
			{
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
			}
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
			{
				task.Execute(databaseCommands, conventions);
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                task.Execute(databaseCommands);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static async Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
			bool failed = false;
	        try
	        {
		        var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
		        var indexesNames = tasks.Select(x => x.IndexName).ToList();
		        // need to make sure we dont replace legacy indexes
				var serverDefinitions = await databaseCommands.GetspecifiedIndexesAsync(indexesNames.ToArray()).ConfigureAwait(false);
		        List<IndexDefinition> definitions = new List<IndexDefinition>();
		        List<string> names = new List<string>();
		        int i = 0;
		        foreach (var task in tasks)
		        {
			        var clientDefinitions = task.CreateIndexDefinition();
			        if (serverDefinitions[i] == null || !task.CurrentOrLegacyIndexDefinitionEquals(conventions, serverDefinitions[i], clientDefinitions))
			        {
				        names.Add(task.IndexName);
				        definitions.Add(clientDefinitions);
			        }
			        i++;
		        }
		        //Only executing missing/updated indexes
		        await databaseCommands.PutIndexesAsync(names, definitions.ToArray()).ConfigureAwait(false);
	        }
			
		        // For old servers that don't have the new entrypoint for executing multiple indexes
	        catch (Exception)
	        {
		        failed = true;		        
	        }
	        if (failed)
	        {
				foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
				{
					try
					{
						await task.ExecuteAsync(databaseCommands, conventions);
					}
					catch (IndexCompilationException e)
					{
						indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
					}

				}
	        }
	        foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(databaseCommands, conventions);
            }

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                await task.ExecuteAsync(databaseCommands);
            }

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			try
			{
				var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
				var indexesNames = tasks.Select(x => x.IndexName).ToList();

				documentStore.DatabaseCommands.PutIndexes(indexesNames, tasks.Select(x=>x.CreateIndexDefinition()).ToArray());
			}
				// For old servers that don't have the new entrypoint for executing multiple indexes
			catch (Exception)
			{
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
			}
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
			{
				task.Execute(documentStore);
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                task.Execute(documentStore);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

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
		public static async Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			bool failed = false;
			try
			{
				var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
				var indexesNames = tasks.Select(x => x.IndexName).ToList();
				await documentStore.AsyncDatabaseCommands.PutIndexesAsync(indexesNames, tasks.Select(x=>x.CreateIndexDefinition()).ToArray()).ConfigureAwait(false);
			}

				// For old servers that don't have the new entrypoint for executing multiple indexes
			catch (Exception)
			{
				failed = true;
			}
			if (failed)
			{
				foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
				{
					try
					{
						await task.ExecuteAsync(documentStore).ConfigureAwait(false);
					}
					catch (IndexCompilationException e)
					{
						indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
					}

				}
			}
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
			{
				await task.ExecuteAsync(documentStore);
			}
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                await task.ExecuteAsync(documentStore);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

		/// <summary>
		/// Creates the indexes found in the specified assembly in side-by-side mode.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static void SideBySideCreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			SideBySideCreateIndexes(catalog, documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog in side-by-side mode.
		/// </summary>
		public static void SideBySideCreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					task.SideBySideExecute(databaseCommands, conventions, minimumEtagBeforeReplace, replaceTimeUtc);
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

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                task.Execute(databaseCommands);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog in side-by-side mode.
		/// </summary>
		public static async Task SideBySideCreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					await task.SideBySideExecuteAsync(databaseCommands, conventions, minimumEtagBeforeReplace, replaceTimeUtc);
				}
				catch (IndexCompilationException e)
				{
					indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
				}

			}

			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
			{
				await task.ExecuteAsync(databaseCommands, conventions);
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                await task.ExecuteAsync(databaseCommands);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog in side-by-side mode.
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void SideBySideCreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					task.SideBySideExecute(documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
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

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                task.Execute(documentStore);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}

		/// <summary>
		/// Creates the indexes found in the specified assembly in side-by-side mode.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static Task SideBySideCreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			return SideBySideCreateIndexesAsync(catalog, documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog in side-by-side mode.
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static async Task SideBySideCreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
		{
			var indexCompilationExceptions = new List<IndexCompilationException>();
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
			{
				try
				{
					await task.SideBySideExecuteAsync(documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
				}
				catch (IndexCompilationException e)
				{
					indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
				}
			}
			foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
			{
				await task.ExecuteAsync(documentStore);
			}

            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractScriptedIndexCreationTask>())
            {
                await task.ExecuteAsync(documentStore);
            }

			if (indexCompilationExceptions.Any())
				throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
		}
	}
}
