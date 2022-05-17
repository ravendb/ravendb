using System;
using System.Globalization;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal abstract class AbstractChangesHandlerProcessorForDeleteConnections<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractChangesHandlerProcessorForDeleteConnections([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract void Disconnect(long connectionId);

    public override ValueTask ExecuteAsync()
    {
        var ids = RequestHandler.GetStringValuesQueryString("id");

        foreach (var idStr in ids)
        {
            if (long.TryParse(idStr, NumberStyles.Any, CultureInfo.InvariantCulture, out long id) == false)
                throw new ArgumentException($"Could not parse query string 'id' header as int64, value was: {idStr}");

            Disconnect(id);
        }

        RequestHandler.NoContentStatus();
        return ValueTask.CompletedTask;
    }
}
