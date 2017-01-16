using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Operations
{
    public interface IAdminOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConvention conventions, JsonOperationContext context);
    }

    public interface IAdminOperation
    {
        RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context);
    }
}