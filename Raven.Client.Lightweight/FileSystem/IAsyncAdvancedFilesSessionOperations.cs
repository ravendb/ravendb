using Raven.Client.RavenFS;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IAsyncAdvancedFilesSessionOperations : IAdvancedFilesSessionOperations
    {

        Task<RavenJObject> DownloadAsync(string filename, Stream destination, long? from = null, long? to = null);
        Task UpdateMetadataAsync(string filename, RavenJObject metadata);
        Task UploadAsync(string filename, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source, Action<string, long> progress);

        Task<string[]> GetFoldersAsync(string from = null, int start = 0, int pageSize = 25);

        Task<SearchResults> GetFilesAsync(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 25);

        Task<Guid> GetServerId();

    }
}
