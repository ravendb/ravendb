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
                if (string.IsNullOrEmpty(id))
                    throw new ArgumentNullException(nameof(id));

                if (Advanced.IsLoaded(id))
                {
                    var entity = await LoadAsync<T>(id, token).ConfigureAwait(false);
                    if (entity == null)
                        return default;
                    
                    var cv = Advanced.GetChangeVectorFor(entity);
                    return (entity, cv);
                }

                if (string.IsNullOrEmpty(changeVector))
                    throw new InvalidOperationException($"The requested document with id '{id} is not loaded into the session and could not conditional load when {nameof(changeVector)} is null or empty.");

                IncrementRequestCount();
                var cmd = new ConditionalGetDocumentsCommand(id, changeVector);
                await Advanced.RequestExecutor.ExecuteAsync(cmd, Advanced.Context, sessionInfo: null, token).ConfigureAwait(false);

                switch (cmd.StatusCode)
                {
                    case HttpStatusCode.NotModified:
                        return (default, changeVector); // value not changed
                    case HttpStatusCode.NotFound:
                        RegisterMissing(id);
                        return default; // value is missing
                }

                var documentInfo = DocumentInfo.GetNewDocumentInfo((BlittableJsonReaderObject)cmd.Result.Results[0]);
                var r = TrackEntity<T>(documentInfo);

                return (r, cmd.Result.ChangeVector);
            }
        }
    }
}
