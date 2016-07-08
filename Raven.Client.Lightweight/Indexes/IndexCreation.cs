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
using Raven.Abstractions.Logging;
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
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();

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
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutIndexes(indexesToAdd);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
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

            CreateTransformers(catalogToGetnIndexingTasksFrom, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static async Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                await databaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
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
                        await task.ExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
                    }
                }
            }

            await CreateTransformersAsync(catalogToGetnIndexingTasksFrom, databaseCommands, conventions).ConfigureAwait(false);

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
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                documentStore.ExecuteIndexes(tasks);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
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

            CreateTransformers(catalogToGetnIndexingTasksFrom, documentStore);

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
            var failed = false;
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, documentStore.Conventions);
                await documentStore.AsyncDatabaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(documentStore.AsyncDatabaseCommands, documentStore.Conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
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

            await CreateTransformersAsync(catalogToGetnIndexingTasksFrom, documentStore).ConfigureAwait(false);

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
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutSideBySideIndexes(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
                {
                    try
                    {
                        task.SideBySideExecute(databaseCommands, conventions, minimumEtagBeforeReplace, replaceTimeUtc);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile side by side index name = " + task.IndexName, e));
                    }
                }
            }

            CreateTransformers(catalogToGetnIndexingTasksFrom, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static async Task SideBySideCreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                await databaseCommands.PutSideBySideIndexesAsync(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
                // For old servers that don't have the new endpoint for executing multiple indexes
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
                        await task.SideBySideExecuteAsync(databaseCommands, conventions, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile side by side index name = " + task.IndexName, e));
                    }
                }
            }

            await CreateTransformersAsync(catalogToGetnIndexingTasksFrom, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        /// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        public static void SideBySideCreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                documentStore.SideBySideExecuteIndexes(tasks, minimumEtagBeforeReplace, replaceTimeUtc);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create side by side indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>())
                {
                    try
                    {
                        task.SideBySideExecute(documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile side by side index name = " + task.IndexName, e));
                    }
                }
            }

            CreateTransformers(catalogToGetnIndexingTasksFrom, documentStore);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
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
            var failed = false;
            try
            {
                var tasks = catalogToGetnIndexingTasksFrom
                    .GetExportedValues<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, documentStore.Conventions);
                await documentStore.AsyncDatabaseCommands.PutSideBySideIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(documentStore.AsyncDatabaseCommands, documentStore.Conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
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
                        await task.SideBySideExecuteAsync(documentStore, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile side by side index name = " + task.IndexName, e));
                    }
                }
            }

            await CreateTransformersAsync(catalogToGetnIndexingTasksFrom, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        private static void CreateTransformers(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
        {
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(documentStore);
            }
        }

        private static void CreateTransformers(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(databaseCommands, conventions);
            }
        }

        private static async Task CreateTransformersAsync(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
        {
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(documentStore).ConfigureAwait(false);
            }
        }

        private static async Task CreateTransformersAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
        }

        public static IndexToAdd[] CreateIndexesToAdd(IEnumerable<AbstractIndexCreationTask> indexCreationTasks, DocumentConvention conventions)
        {
            var indexesToAdd = indexCreationTasks
                .Select(x =>
                {
                    x.Conventions = conventions;
                    return new IndexToAdd
                    {
                        Definition = x.CreateIndexDefinition(),
                        Name = x.IndexName,
                        Priority = x.Priority ?? IndexingPriority.Normal
                    };
                })
                .ToArray();

            return indexesToAdd;
        }
    }
}