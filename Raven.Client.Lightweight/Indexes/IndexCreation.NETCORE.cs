#if DNXCORE50
//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Composition.Convention;
using System.Composition.Hosting;
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
    public static partial class IndexCreation
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IndexCreation));

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
        /// <param name="documentStore">The document store.</param>
        public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
        {
            var configuration = CreateConfiguration(assemblyToScanForIndexingTasks);

            using (var container = configuration.CreateContainer())
            {
                CreateIndexes(container, documentStore);
            }
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static void CreateIndexes(CompositionHost container, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutIndexes(indexesToAdd);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            CreateTransformers(container, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        public static async Task CreateIndexesAsync(CompositionHost container, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
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
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            await CreateTransformersAsync(container, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        /// <param name="container">The container to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        public static void CreateIndexes(CompositionHost container, IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
                    .ToList();

                documentStore.ExecuteIndexes(tasks);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            CreateTransformers(container, documentStore);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
        /// <param name="documentStore">The document store.</param>
        public static async Task CreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
        {
            var configuration = CreateConfiguration(assemblyToScanForIndexingTasks);

            using (var container = configuration.CreateContainer())
            {
                await CreateIndexesAsync(container, documentStore).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog
        /// </summary>
        /// <param name="container">The container to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        public static async Task CreateIndexesAsync(CompositionHost container, IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
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
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            await CreateTransformersAsync(container, documentStore).ConfigureAwait(false);

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
        public static void SideBySideCreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var configuration = CreateConfiguration(assemblyToScanForIndexingTasks);

            using (var container = configuration.CreateContainer())
            {
                SideBySideCreateIndexes(container, documentStore, minimumEtagBeforeReplace, replaceTimeUtc);
            }
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static void SideBySideCreateIndexes(CompositionHost container, IDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutSideBySideIndexes(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            CreateTransformers(container, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static async Task SideBySideCreateIndexesAsync(CompositionHost container, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
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
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            await CreateTransformersAsync(container, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        /// <param name="container">The container to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static void SideBySideCreateIndexes(CompositionHost container, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
                    .ToList();

                documentStore.SideBySideExecuteIndexes(tasks, minimumEtagBeforeReplace, replaceTimeUtc);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create side by side indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            CreateTransformers(container, documentStore);

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
        public static async Task SideBySideCreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var configuration = CreateConfiguration(assemblyToScanForIndexingTasks);

            using (var container = configuration.CreateContainer())
            {
                await SideBySideCreateIndexesAsync(container, documentStore, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        /// <param name="container">The container to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        public static async Task SideBySideCreateIndexesAsync(CompositionHost container, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            var failed = false;
            try
            {
                var tasks = container
                    .GetExports<AbstractIndexCreationTask>()
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
                foreach (var task in container.GetExports<AbstractIndexCreationTask>())
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

            await CreateTransformersAsync(container, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        private static void CreateTransformers(CompositionHost container, IDocumentStore documentStore)
        {
            foreach (var task in container.GetExports<AbstractTransformerCreationTask>())
            {
                task.Execute(documentStore);
            }
        }

        private static void CreateTransformers(CompositionHost container, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in container.GetExports<AbstractTransformerCreationTask>())
            {
                task.Execute(databaseCommands, conventions);
            }
        }

        private static async Task CreateTransformersAsync(CompositionHost container, IDocumentStore documentStore)
        {
            foreach (var task in container.GetExports<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(documentStore).ConfigureAwait(false);
            }
        }

        private static async Task CreateTransformersAsync(CompositionHost container, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in container.GetExports<AbstractTransformerCreationTask>())
            {
                await task.ExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
        }

        private static ContainerConfiguration CreateConfiguration(Assembly assemblyToScan)
        {
            var conventions = new ConventionBuilder();

            conventions
                .ForTypesDerivedFrom<AbstractIndexCreationTask>()
                .Export<AbstractIndexCreationTask>()
                .Shared();

            conventions
                .ForTypesDerivedFrom<AbstractTransformerCreationTask>()
                .Export<AbstractTransformerCreationTask>()
                .Shared();

            return new ContainerConfiguration()
                .WithAssembly(assemblyToScan, conventions);
        }
    }
}
#endif