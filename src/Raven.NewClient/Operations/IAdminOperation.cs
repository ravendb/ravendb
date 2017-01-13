using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;

namespace Raven.NewClient.Operations
{
    public interface IAdminOperation<T>
    {
        RavenCommand<T> GetCommand(DocumentConvention conventions);
    }

    public interface IAdminOperation
    {
        RavenCommand<object> GetCommand(DocumentConvention conventions);
    }
}