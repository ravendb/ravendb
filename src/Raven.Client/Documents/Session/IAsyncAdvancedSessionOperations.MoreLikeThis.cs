using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;

namespace Raven.Client.Documents.Session
{
    public partial interface IAsyncAdvancedSessionOperations
    {
        Task<List<T>> MoreLikeThisAsync<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new();


        Task<List<T>> MoreLikeThisAsync<T>(string index, string documentId);

        Task<List<T>> MoreLikeThisAsync<T>(MoreLikeThisQuery query);
    }
}
