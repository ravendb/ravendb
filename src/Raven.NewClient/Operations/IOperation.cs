using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Operations
{
    public interface IOperation
    {
        RavenCommand<OperationIdResult> GetCommand(DocumentConvention conventions, JsonOperationContext context);
    }
}