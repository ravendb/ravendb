using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        /// <inheritdoc />
        public async Task<(T Entity, string ChangeVector)> ConditionalLoadAsync<T>(string id, string changeVector, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                if (Advanced.IsLoaded(id))
                {
                    var e = await LoadAsync<T>(id, token).ConfigureAwait(false);
                    var cv = Advanced.GetChangeVectorFor(e);
                    return (e, cv);
                }

                if (changeVector == null)
                    return default;

                IncrementRequestCount();
                var cmd = new ConditionalGetDocumentsCommand(id, changeVector);
                await Advanced.RequestExecutor.ExecuteAsync(cmd, Advanced.Context, sessionInfo: null, token).ConfigureAwait(false);

                switch (cmd.StatusCode)
                {
                    case HttpStatusCode.NotModified:
                        return (default, changeVector); // value not changed
                    case HttpStatusCode.NotFound:
                        return default; // value is missing
                }

                if (cmd.Result.Results.Length == 0)
                    return (default, cmd.Result.ChangeVector);

                var documentInfo = DocumentInfo.GetNewDocumentInfo((BlittableJsonReaderObject)cmd.Result.Results[0]);
                var r = TrackEntity<T>(documentInfo);

                return (r, cmd.Result.ChangeVector);
            }
        }
    }
}
