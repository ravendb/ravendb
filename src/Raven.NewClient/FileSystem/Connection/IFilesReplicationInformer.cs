using System.Threading.Tasks;
using Raven.NewClient.Client.Connection;

namespace Raven.NewClient.Client.FileSystem.Connection
{
    public interface IFilesReplicationInformer : IReplicationInformerBase<IAsyncFilesCommands>
    {
        /// <summary>
        /// Updates replication information if needed
        /// </summary>
        Task UpdateReplicationInformationIfNeeded(IAsyncFilesCommands commands);
    }
}
