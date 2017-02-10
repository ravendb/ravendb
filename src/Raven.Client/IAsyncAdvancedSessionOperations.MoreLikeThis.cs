using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document
{
    public partial interface IAsyncAdvancedSessionOperations
    {
        Task<List<T>> MoreLikeThisAsync<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new();

        Task<List<T>> MoreLikeThisAsync<T, TIndexCreator>(MoreLikeThisQuery query) where TIndexCreator : AbstractIndexCreationTask, new();

        Task<List<T>> MoreLikeThisAsync<TTransformer, T, TIndexCreator>(string documentId, Dictionary<string, object> transformerParameters = null)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        Task<List<T>> MoreLikeThisAsync<TTransformer, T, TIndexCreator>(MoreLikeThisQuery query)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        Task<List<T>> MoreLikeThisAsync<T>(string index, string documentId, string transformer = null, Dictionary<string, object> transformerParameters = null);

        Task<List<T>> MoreLikeThisAsync<T>(MoreLikeThisQuery query);
    }
}