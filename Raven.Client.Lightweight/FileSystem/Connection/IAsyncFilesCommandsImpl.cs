using Raven.Client.Connection;

namespace Raven.Client.FileSystem.Connection
{
    public interface IAsyncFilesCommandsImpl : IAsyncFilesCommands
    {   
        string ServerUrl { get; }

        HttpJsonRequestFactory RequestFactory { get; }

        IFilesReplicationInformer ReplicationInformer { get; }
    }
}
