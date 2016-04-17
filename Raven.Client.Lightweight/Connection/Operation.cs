using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    public class Operation
    {
        private readonly Func<long, Task<RavenJToken>> statusFetcher;
        private readonly long id;
        private readonly RavenJToken state;
        private readonly bool done;

        public Operation(long id, RavenJToken state)
        {
            this.id = id;
            this.state = state;
            done = true;
        }

        public Operation(Func<long, Task<RavenJToken>> statusFetcher, long id)
        {
            this.statusFetcher = statusFetcher;
            this.id = id;
        }

        public Operation(AsyncServerClient asyncServerClient, long id)
            : this(asyncServerClient.GetOperationStatusAsync, id)
        {
        }

        public Action<BulkOperationProgress> OnProgressChanged;

        public virtual async Task<RavenJToken> WaitForCompletionAsync()
        {
            if (done)
                return state;
            if (statusFetcher == null)
                throw new InvalidOperationException("Cannot use WaitForCompletionAsync() when the operation was executed syncronously");

            while (true)
            {
                var status = await statusFetcher(id).ConfigureAwait(false);
                if (status == null)
                    return null;

                var onProgress = OnProgressChanged;

                if (onProgress != null)
                {
                    var progressToken = status.Value<RavenJToken>("OperationProgress");

                    if (progressToken != null && progressToken.Equals(RavenJValue.Null) == false)
                    {
                        onProgress(new BulkOperationProgress
                        {
                            TotalEntries = progressToken.Value<int>("TotalEntries"),
                            ProcessedEntries = progressToken.Value<int>("ProcessedEntries")
                        });
                    }
                }

                if (status.Value<bool>("Completed"))
                {
                    var faulted = status.Value<bool>("Faulted");
                    if (faulted)
                    {
                        var error = status.Value<RavenJToken>("State");
                        var errorMessage = error.Value<string>("Error");
                        throw new InvalidOperationException("Operation failed: " + errorMessage);
                    }

                    var canceled = status.Value<bool>("Canceled");
                    if (canceled)
                    {
                        var error = status.Value<RavenJToken>("State");
                        var errorMessage = error.Value<string>("Error");
                        throw new InvalidOperationException("Operation canceled: " + errorMessage);
                    }

                    return status.Value<RavenJToken>("State");
                }
                    

                await Task.Delay(500).ConfigureAwait(false);
            }
        }

        public virtual RavenJToken WaitForCompletion()
        {
            return AsyncHelpers.RunSync(WaitForCompletionAsync);
        }
    }
}
