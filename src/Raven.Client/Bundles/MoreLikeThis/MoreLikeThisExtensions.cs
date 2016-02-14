using System;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;

namespace Raven.Client.Bundles.MoreLikeThis
{
    public static class MoreLikeThisExtensions
    {
        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, null, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, MoreLikeThisQuery parameters) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, null, parameters);
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId)
        {
            return MoreLikeThis<T>(advancedSession, index, null, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static T[] MoreLikeThis<TTransformer, T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            var transformer = new TTransformer();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, transformer.TransformerName, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static T[] MoreLikeThis<TTransformer, T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, MoreLikeThisQuery parameters) 
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            var transformer = new TTransformer();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, transformer.TransformerName, parameters);
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string transformer, string documentId)
        {
            return MoreLikeThis<T>(advancedSession, index, transformer, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string transformer, MoreLikeThisQuery parameters)
        {
            if (string.IsNullOrEmpty(index))
                throw new ArgumentException("Index name cannot be null or empty", "index");

            parameters.IndexName = index;
            parameters.ResultsTransformer = transformer;

            // /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
            var cmd = ((DocumentSession) advancedSession).DatabaseCommands;

            var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);
            inMemoryDocumentSessionOperations.IncrementRequestCount();

            var loadOperation = new LoadOperation(inMemoryDocumentSessionOperations, cmd.DisableAllCaching, null, null);
            LoadResult loadResult;
            do
            {
                loadOperation.LogOperation();
                using (loadOperation.EnterLoadContext())
                {
                    loadResult = cmd.MoreLikeThis(parameters);
                }
            } while (loadOperation.SetResult(loadResult));

            return loadOperation.Complete<T>();
        }

        public static Task<T[]> MoreLikeThisAsync<T, TIndexCreator>(this IAsyncAdvancedSessionOperations advancedSession, string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThisAsync<T>(advancedSession, indexCreator.IndexName, null, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static Task<T[]> MoreLikeThisAsync<T, TIndexCreator>(this IAsyncAdvancedSessionOperations advancedSession, MoreLikeThisQuery parameters) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThisAsync<T>(advancedSession, indexCreator.IndexName, null, parameters);
        }

        public static Task<T[]> MoreLikeThisAsync<T>(this IAsyncAdvancedSessionOperations advancedSession, string index, string documentId)
        {
            return MoreLikeThisAsync<T>(advancedSession, index, null, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static Task<T[]> MoreLikeThisAsync<TTransformer, T, TIndexCreator>(this IAsyncAdvancedSessionOperations advancedSession, string documentId)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            var transformer = new TTransformer();
            return MoreLikeThisAsync<T>(advancedSession, indexCreator.IndexName, transformer.TransformerName, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static Task<T[]> MoreLikeThisAsync<TTransformer, T, TIndexCreator>(this IAsyncAdvancedSessionOperations advancedSession, MoreLikeThisQuery parameters)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            var transformer = new TTransformer();
            return MoreLikeThisAsync<T>(advancedSession, indexCreator.IndexName, transformer.TransformerName, parameters);
        }

        public static Task<T[]> MoreLikeThisAsync<T>(this IAsyncAdvancedSessionOperations advancedSession, string index, string transformer, string documentId)
        {
            return MoreLikeThisAsync<T>(advancedSession, index, transformer, new MoreLikeThisQuery
            {
                DocumentId = documentId
            });
        }

        public static async Task<T[]> MoreLikeThisAsync<T>(this IAsyncAdvancedSessionOperations advancedSession, string index, string transformer, MoreLikeThisQuery parameters)
        {
            if (string.IsNullOrEmpty(index))
                throw new ArgumentException("Index name cannot be null or empty", "index");

            parameters.IndexName = index;
            parameters.ResultsTransformer = transformer;

            // /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
            var cmd = ((AsyncDocumentSession)advancedSession).AsyncDatabaseCommands;

            var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);
            inMemoryDocumentSessionOperations.IncrementRequestCount();

            var loadOperation = new LoadOperation(inMemoryDocumentSessionOperations, cmd.DisableAllCaching, null, null);
            LoadResult loadResult;
            do
            {
                loadOperation.LogOperation();
                using (loadOperation.EnterLoadContext())
                {
                    loadResult = await cmd.MoreLikeThisAsync(parameters).ConfigureAwait(false);
                }
            } while (loadOperation.SetResult(loadResult));

            return loadOperation.Complete<T>();
        }
    }
}
