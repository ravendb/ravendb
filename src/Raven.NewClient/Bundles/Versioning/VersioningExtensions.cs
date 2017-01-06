using System;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Extensions;
using Sparrow.Json;

namespace Raven.NewClient.Client.Bundles.Versioning
{
    public static class VersioningExtensions
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static T[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
        {
            var getRevisionsOperation = new GetRevisionOperation();
            var command = getRevisionsOperation.CreateRequest(id, start, pageSize);
            session.RequestExecuter.Execute(command, session.Context);
            return ProcessResults<T>(session, command);
        }

        private static T[] ProcessResults<T>(IAdvancedDocumentSessionOperations session, GetRevisionCommand command)
        {
            var results = command.Result.Results;
            var res = new T[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                var document = (BlittableJsonReaderObject) results[i];
                var metadata = document.GetMetadata();
                var id = metadata.GetId();

                res[i] = (T) session.ConvertToEntity(typeof(T), id, document);
            }
            return res;
        }

        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static async Task<T[]> GetRevisionsForAsync<T>(this IAsyncAdvancedSessionOperations session, string id, int start = 0, int pageSize = 25)
        {
            var getRevisionsOperation = new GetRevisionOperation();
            var command = getRevisionsOperation.CreateRequest(id, start, pageSize);
            await session.RequestExecuter.ExecuteAsync(command, session.Context).ConfigureAwait(false);
            return ProcessResults<T>(session, command);
        }
    }
}
