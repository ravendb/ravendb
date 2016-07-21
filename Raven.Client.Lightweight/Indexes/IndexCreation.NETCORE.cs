#if DNXCORE50
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
        public static void CreateIndexes(Assembly assemblyToScan, IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                documentStore.ExecuteIndexes(tasks);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            CreateTransformers(assemblyToScan, documentStore);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static void CreateIndexes(Assembly assemblyToScan, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutIndexes(indexesToAdd);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            CreateTransformers(assemblyToScan, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static async Task CreateIndexesAsync(Assembly assemblyToScan, IDocumentStore documentStore)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                await documentStore.ExecuteIndexesAsync(tasks).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            await CreateTransformersAsync(assemblyToScan, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static async Task CreateIndexesAsync(Assembly assemblyToScan, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                await databaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            await CreateTransformersAsync(assemblyToScan, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static void SideBySideCreateIndexes(Assembly assemblyToScan, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                documentStore.SideBySideExecuteIndexes(tasks, minimumEtagBeforeReplace, replaceTimeUtc);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create side by side indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            CreateTransformers(assemblyToScan, documentStore);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly in side-by-side mode.
        /// </summary>
        public static void SideBySideCreateIndexes(Assembly assemblyToScan, IDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                databaseCommands.PutSideBySideIndexes(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc);

                foreach (var task in tasks)
                    task.AfterExecute(databaseCommands, conventions);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            CreateTransformers(assemblyToScan, databaseCommands, conventions);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified catalog in side-by-side mode.
        /// </summary>
        public static async Task SideBySideCreateIndexesAsync(Assembly assemblyToScan, IDocumentStore documentStore, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                await documentStore.SideBySideExecuteIndexesAsync(tasks, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                Log.InfoException("Could not create side by side indexes in one shot (maybe using older version of RavenDB ?)", ex);
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            await CreateTransformersAsync(assemblyToScan, documentStore).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly in side-by-side mode.
        /// </summary>
        public static async Task SideBySideCreateIndexesAsync(Assembly assemblyToScan, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var tasks = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan)
                    .ToList();

                var indexesToAdd = CreateIndexesToAdd(tasks, conventions);
                await databaseCommands.PutSideBySideIndexesAsync(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);

                foreach (var task in tasks)
                    await task.AfterExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception)
            {
                foreach (var task in GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan))
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

            await CreateTransformersAsync(assemblyToScan, databaseCommands, conventions).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more side by side indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        private static void CreateTransformers(Assembly assemblyToScan, IDocumentStore documentStore)
        {
            foreach (var task in GetAllInstancesOfType<AbstractTransformerCreationTask>(assemblyToScan))
            {
                task.Execute(documentStore);
            }
        }

        private static void CreateTransformers(Assembly assemblyToScan, IDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in GetAllInstancesOfType<AbstractTransformerCreationTask>(assemblyToScan))
            {
                task.Execute(databaseCommands, conventions);
            }
        }

        private static async Task CreateTransformersAsync(Assembly assemblyToScan, IDocumentStore documentStore)
        {
            foreach (var task in GetAllInstancesOfType<AbstractTransformerCreationTask>(assemblyToScan))
            {
                await task.ExecuteAsync(documentStore).ConfigureAwait(false);
            }
        }

        private static async Task CreateTransformersAsync(Assembly assemblyToScan, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions)
        {
            foreach (var task in GetAllInstancesOfType<AbstractTransformerCreationTask>(assemblyToScan))
            {
                await task.ExecuteAsync(databaseCommands, conventions).ConfigureAwait(false);
            }
        }

        private static IEnumerable<TType> GetAllInstancesOfType<TType>(Assembly assembly)
        {
            foreach (var type in assembly.GetTypes()
                .Where(x => x.GetTypeInfo().IsClass && x.GetTypeInfo().IsAbstract == false && x.GetTypeInfo().IsSubclassOf(typeof(TType))))
            {
                yield return (TType)Activator.CreateInstance(type);
            }
        }
    }
}
#endif