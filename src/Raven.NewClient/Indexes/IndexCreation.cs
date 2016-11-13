//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Data.Indexes;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Exceptions;

namespace Raven.NewClient.Client.Indexes
{
    /// <summary>
    /// Helper class for creating indexes from implementations of <see cref="AbstractIndexCreationTask"/>.
    /// </summary>
    public static class IndexCreation
    {
        private readonly static ILog Log = LogManager.GetLogger(typeof(IndexCreation));

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
        /// <param name="documentStore">The document store.</param>
        public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
        {
            throw new NotImplementedException("Figure out how to get the relevant types from the assembly");

            //CreateIndexes(catalog, documentStore);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static void CreateIndexes(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers , IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var indexesToAdd = CreateIndexesToAdd(indexes, conventions);
                databaseCommands.PutIndexes(indexesToAdd);

                foreach (var task in indexes)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in indexes)
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

            CreateTransformers(transformers, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static async Task CreateIndexesAsync(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
            
                var indexesToAdd = CreateIndexesToAdd(indexes, conventions);
                await databaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in indexes)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                failed = true;
            }
            if (failed)
            {
                foreach (var task in indexes)
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

            await CreateTransformersAsync(transformers, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        /// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        public static void CreateIndexes(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers, IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
              
                documentStore.ExecuteIndexes(indexes);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in indexes)
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

            CreateTransformers(transformers, documentStore);

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
            throw new NotImplementedException("Figure out how to get the relevant types from the assembly");

            //return CreateIndexesAsync(catalog, documentStore);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        /// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        public static async Task CreateIndexesAsync(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers , IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
       

                var indexesToAdd = CreateIndexesToAdd(indexes, documentStore.Conventions);
                await documentStore.AsyncDatabaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in indexes)
                    await task.AfterExecuteAsync(documentStore.AsyncDatabaseCommands, documentStore.Conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                failed = true;
            }
            if (failed)
            {
                foreach (var task in indexes)
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

            await CreateTransformersAsync(transformers, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly in side-by-side mode.
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static void SideBySideCreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            throw new NotImplementedException("Figure out how to get the relevant types from the assembly");

            //SideBySideCreateIndexes(catalog, documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static void SideBySideCreateIndexes(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers, IDatabaseCommands databaseCommands, DocumentConvention conventions, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
            
                var indexesToAdd = CreateIndexesToAdd(indexes, conventions);
                databaseCommands.PutSideBySideIndexes(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc);

                foreach (var task in indexes)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in indexes)
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

            CreateTransformers(transformers, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static async Task SideBySideCreateIndexesAsync(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
           

                var indexesToAdd = CreateIndexesToAdd(indexes, conventions);
                await databaseCommands.PutSideBySideIndexesAsync(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);

                foreach (var task in indexes)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
                // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                failed = true;
            }
            if (failed)
            {
                foreach (var task in indexes)
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

            await CreateTransformersAsync(transformers, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        /// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static void SideBySideCreateIndexes(IList<AbstractIndexCreationTask> indexes, IList<AbstractTransformerCreationTask> transformers, IDocumentStore documentStore, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
               
                documentStore.SideBySideExecuteIndexes(indexes, minimumEtagBeforeReplace, replaceTimeUtc);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create side by side indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in indexes)
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

            CreateTransformers(transformers, documentStore);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly in side-by-side mode.
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static Task SideBySideCreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            throw new NotImplementedException("Figure out how to get the relevant types from the assembly");
            //return SideBySideCreateIndexesAsync(catalog, documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static async Task SideBySideCreateIndexesAsync(
            IList<AbstractIndexCreationTask> indexes,
            IList<AbstractTransformerCreationTask> transformers,
            IDocumentStore documentStore, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var indexesToAdd = CreateIndexesToAdd(indexes, documentStore.Conventions);
                await documentStore.AsyncDatabaseCommands.PutSideBySideIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in indexes)
                    await task.AfterExecuteAsync(documentStore.AsyncDatabaseCommands, documentStore.Conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                failed = true;
            }
            if (failed)
            {
                foreach (var task in indexes)
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

            await CreateTransformersAsync(transformers, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        private static void CreateTransformers(IEnumerable<AbstractTransformerCreationTask> transformers, IDocumentStore documentStore)
        {
            foreach (var task in transformers)
            {
                task.Execute(documentStore);
            }
        }

        private static void CreateTransformers(IEnumerable<AbstractTransformerCreationTask> transformers, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in transformers)
            {
                task.Execute(databaseCommands, conventions);
            }
        }

        private static async Task CreateTransformersAsync(IEnumerable<AbstractTransformerCreationTask> transformers, IDocumentStore documentStore)
        {
            foreach (var task in transformers)
            {
                await task.ExecuteAsync(documentStore).ConfigureAwait(false);
            }
        }

        private static async Task CreateTransformersAsync(IEnumerable<AbstractTransformerCreationTask> transformers, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in transformers)
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