using System.Threading.Tasks;
using Raven.Client.Connection;

namespace Raven.Client.FileSystem.Connection
{
    public interface IFilesReplicationInformer : IReplicationInformerBase<IAsyncFilesCommands>
    {
        /// <summary>
        /// Updates replication information if needed
        /// </summary>
        Task UpdateReplicationInformationIfNeeded(IAsyncFilesCommands commands);
    }
}
