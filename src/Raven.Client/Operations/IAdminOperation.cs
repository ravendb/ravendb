using Raven.Client.Commands;
using Raven.Client.Document;
using Sparrow.Json;

namespace Raven.Client.Operations
{
    public interface IAdminOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }

    public interface IAdminOperation
    {
        RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context);
    }
}