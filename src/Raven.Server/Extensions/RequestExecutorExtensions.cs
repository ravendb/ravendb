using System.Threading;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Extensions
{
    public static class RequestExecutorExtensions
    {
        public static void ExecuteWithCancellationToken<TResult>(
            this RequestExecutor requestExecutor,
            RavenCommand<TResult> command,
            JsonOperationContext context,
            CancellationToken token)
        {
            AsyncHelpers.RunSync(() => requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token));
        }
    }
}
