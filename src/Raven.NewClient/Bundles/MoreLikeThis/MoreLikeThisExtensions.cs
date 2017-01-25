using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Async;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Bundles.MoreLikeThis
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
            parameters.Transformer = transformer;

            // /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
            var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);
            inMemoryDocumentSessionOperations.IncrementRequestCount();

            var loadOperation = new LoadOperation(inMemoryDocumentSessionOperations, true);

            var command = new MoreLikeThisCommand()
            {
                Query = parameters
            };
            advancedSession.RequestExecuter.Execute(command, advancedSession.Context);

            var result = command.Result;
            loadOperation.SetResult(result);

            return loadOperation.GetDocuments<T>().Values.ToArray();
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
            parameters.Transformer = transformer;

            var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);
            inMemoryDocumentSessionOperations.IncrementRequestCount();

            var loadOperation = new LoadOperation(inMemoryDocumentSessionOperations, true);

            var command = new MoreLikeThisCommand()
            {
                Query = parameters
            };
            await advancedSession.RequestExecuter.ExecuteAsync(command, advancedSession.Context).ConfigureAwait(false);

            var result = command.Result;
            loadOperation.SetResult(result);

            return loadOperation.GetDocuments<T>().Values.ToArray();
        }
    }
}
