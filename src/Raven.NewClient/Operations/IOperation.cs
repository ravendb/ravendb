using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Operations
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