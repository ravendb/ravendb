using Sparrow.Json;

namespace Raven.Server.ServerWide.Commands;

public interface IContextResultCommand
{
    public JsonOperationContext ContextToWriteResult { set; }

    public object WriteResult(object result);
}
