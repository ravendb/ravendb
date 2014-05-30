using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Connection
{
    public interface IAsyncFilesCommandsImpl : IAsyncFilesCommands
    {
        Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null);

        Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId);        
        Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag);

        Task<SignatureManifest> GetRdcManifestAsync(string path);
        Task<RdcStats> GetRdcStatsAsync();

        Task<IEnumerable<SynchronizationConfirmation>> ConfirmFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles);
    }
}
