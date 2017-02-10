using Raven.Client.Commands;
using Raven.Client.Document;
using Sparrow.Json;

namespace Raven.Client.Operations
{
    public interface IOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConvention conventions, JsonOperationContext context);
    }

    public interface IOperation
    {
        RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context);
    }
}