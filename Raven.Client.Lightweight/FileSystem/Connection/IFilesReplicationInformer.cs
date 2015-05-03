using Raven.Client.Connection;

namespace Raven.Client.FileSystem.Connection
{
    public interface IFilesReplicationInformer : IReplicationInformerBase<IAsyncFilesCommands>
    {
    }
}
