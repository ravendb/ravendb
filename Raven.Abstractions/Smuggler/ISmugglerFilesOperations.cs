using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{

    public interface ISmugglerFilesOperations
    {
        SmugglerFilesOptions Options { get; }


        Task<FileSystemStats[]> GetStats();
        Task<string> GetVersion(FilesConnectionStringOptions server);        

        LastFilesEtagsInfo FetchCurrentMaxEtags();


        Task<IAsyncEnumerator<FileHeader>> GetFiles(FilesConnectionStringOptions src, Etag lastEtag, int take);
        Task<Stream> DownloadFile(FileHeader file);
        Task PutFiles(Stream files, RavenJObject metadata);                

        
        void Initialize(SmugglerFilesOptions options);

        void Configure(SmugglerFilesOptions options);

        void ShowProgress(string format, params object[] args);
    }
}
