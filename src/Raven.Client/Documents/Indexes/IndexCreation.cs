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
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Sparrow.Logging;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Helper class for creating indexes from implementations of <see cref="AbstractIndexCreationTask"/>.
    /// </summary>
    public static class IndexCreation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(IndexCreation).FullName);

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static void CreateIndexes(Assembly assemblyToScan, IDocumentStore store, DocumentConventions conventions = null)
        {
            AsyncHelpers.RunSync(() => CreateIndexesAsync(assemblyToScan, store, conventions));
        }

        /// <summary>
        /// Creates the indexes found in the specified assembly.
        /// </summary>
        public static Task CreateIndexesAsync(Assembly assemblyToScan, IDocumentStore store, DocumentConventions conventions = null, CancellationToken token = default(CancellationToken))
        {
            var indexes = GetAllInstancesOfType<AbstractIndexCreationTask>(assemblyToScan);
            var transformers = GetAllInstancesOfType<AbstractTransformerCreationTask>(assemblyToScan);

            return CreateIndexesAsync(indexes, transformers, store, conventions, token);
        }

        public static void CreateIndexes(IEnumerable<AbstractIndexCreationTask> indexes, IEnumerable<AbstractTransformerCreationTask> transformers, IDocumentStore store, DocumentConventions conventions = null)
        {
            AsyncHelpers.RunSync(() => CreateIndexesAsync(indexes, transformers, store, conventions));
        }

        public static async Task CreateIndexesAsync(IEnumerable<AbstractIndexCreationTask> indexes, IEnumerable<AbstractTransformerCreationTask> transformers, IDocumentStore store, DocumentConventions conventions = null, CancellationToken token = default(CancellationToken))
        {
            var indexesList = indexes?.ToList() ?? new List<AbstractIndexCreationTask>();
            var transformersList = transformers?.ToList() ?? new List<AbstractTransformerCreationTask>();

            if (conventions == null)
                conventions = store.Conventions;

            var indexCompilationExceptions = new List<IndexCompilationException>();
            try
            {
                var indexesToAdd = CreateIndexesToAdd(indexesList, conventions);
                await store.Admin.SendAsync(new PutIndexesOperation(indexesToAdd), token).ConfigureAwait(false);
            }
            // For old servers that don't have the new endpoint for executing multiple indexes
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not create indexes in one shot (maybe using older version of RavenDB ?)", ex);
                }

                foreach (var task in indexesList)
                {
                    try
                    {
                        await task.ExecuteAsync(store, conventions, token).ConfigureAwait(false);
                    }
                    catch (IndexCompilationException e)
                    {
                        indexCompilationExceptions.Add(new IndexCompilationException("Failed to compile index name = " + task.IndexName, e));
                    }
                }
            }

            foreach (var task in transformersList)
                await task.ExecuteAsync(store, conventions, token).ConfigureAwait(false);

            if (indexCompilationExceptions.Any())
                throw new AggregateException("Failed to create one or more indexes. Please see inner exceptions for more details.", indexCompilationExceptions);
        }

        internal static IndexDefinition[] CreateIndexesToAdd(IEnumerable<AbstractIndexCreationTask> indexCreationTasks, DocumentConventions conventions)
        {
            var indexesToAdd = indexCreationTasks
                .Select(x =>
                {
                    x.Conventions = conventions;
                    var definition = x.CreateIndexDefinition();
                    definition.Name = x.IndexName;
                    definition.Priority = x.Priority ?? IndexPriority.Normal;
                    return definition;
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