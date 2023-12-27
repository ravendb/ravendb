using Sparrow.Json;

namespace Raven.Server.ServerWide.Commands;

public interface IBlittableResultCommand
{
    public JsonOperationContext ContextToWriteResult { set; }

    public object WriteResult(object result);
}
