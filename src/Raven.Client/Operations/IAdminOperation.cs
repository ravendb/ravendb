using Raven.Client.Commands;
using Raven.Client.Document;
using Sparrow.Json;

namespace Raven.Client.Operations
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