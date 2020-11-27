//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Logging;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Helper class for creating indexes from implementations of <see cref="AbstractIndexCreationTask"/>.
    /// </summary>
    public static class IndexCreation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Client", typeof(IndexCreation).FullName);

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static void CreateIndexes(Assembly assemblyToScan, IDocumentStore store, DocumentConventions conventions = null, string database = null)
        {
            AsyncHelpers.RunSync(() => CreateIndexesAsync(assemblyToScan, store, conventions, database));
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static Task CreateIndexesAsync(Assembly assemblyToScan, IDocumentStore store, DocumentConventions conventions = null, string database = null, CancellationToken token = default(CancellationToken))
        {
            var indexes = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan);

            return CreateIndexesAsync(indexes, store, conventions, database, token);
        }

        public static void CreateIndexes(IEnumerable<AbstractIndexCreationTask> indexes, IDocumentStore store, DocumentConventions conventions = null, string database = null)
        {
            AsyncHelpers.RunSync(() => CreateIndexesAsync(indexes, store, conventions, database));
        }

        public static async Task CreateIndexesAsync(IEnumerable<AbstractIndexCreationTask> indexes, IDocumentStore store, DocumentConventions conventions = null, string database = null, CancellationToken token = default(CancellationToken))
        {
            database = store.GetDatabase(database);

            var indexesList = indexes?.ToList() ?? new List<AbstractIndexCreationTask>();

            if (conventions == null)
                conventions = store.Conventions;

            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var indexesToAdd = CreateIndexesToAdd(indexesList, conventions);
                await store.Maintenance.ForDatabase(database).SendAsync(new PutIndexesOperation(indexesToAdd), token).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                }

                foreach (var task in indexesList)
                {
                    try
                    {
                        await task.ExecuteAsync(store, conventions, database, token).ConfigureAwait(false);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
                    }
                }
            }

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        internal static IndexDefinition[] CreateIndexesToAdd(IEnumerable<AbstractIndexCreationTask> indexCreationTasks, DocumentConventions conventions)
        {
            var indexesToAdd = indexCreationTasks
                .Select(x =>
                {
                    var oldConventions = x.Conventions;

                    try
                    {
                        x.Conventions = conventions;
                        var definition = x.CreateIndexDefinition();
                        definition.Name = x.IndexName;
                        definition.Priority = x.Priority ?? IndexPriority.Normal;
                        return definition;
                    }
                    finally
                    {
                        x.Conventions = oldConventions;
                    }
                })
                .ToArray();

            return indexesToAdd;
        }

        private static IEnumerable<TType> GetAllInstancesOfType<TType>(Assembly assembly)
        {
            foreach (var type in assembly.GetTypes()
                .Where(x =>
                x.GetTypeInfo().IsClass &&
                x.GetTypeInfo().IsAbstract == false &&
                x.GetTypeInfo().IsSubclassOf(typeof(TType))))
            {
                yield return (TType)Activator.CreateInstance(type);
            }
        }
    }
}
