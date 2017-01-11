using Raven.NewClient.Client.Commands;

namespace Raven.NewClient.Operations
{
    public interface IAdminOperation<T>
    {
        RavenCommand<T> GetCommand();
    }

    public interface IAdminOperation
    {
        RavenCommand<object> GetCommand();
    }
}