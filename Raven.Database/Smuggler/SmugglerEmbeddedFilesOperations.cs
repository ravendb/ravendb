using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.Server.RavenFS;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
    public class SmugglerEmbeddedFilesOperations : ISmugglerFilesOperations
    {
        private readonly RavenFileSystem filesystem;

        public SmugglerFilesOptions Options { get; private set; }

        public Action<string> Progress { get; set; }

        public SmugglerEmbeddedFilesOperations(RavenFileSystem filesystem)
        {
            this.filesystem = filesystem;
        }
        
        public void Initialize(SmugglerFilesOptions options)
        {
            this.Options = options;
        }

        public void Configure(SmugglerFilesOptions options)
        {
            throw new NotImplementedException();
        }

        public Task<FileSystemStats[]> GetStats()
        {
            throw new NotImplementedException();
        }

        public Task<string> GetVersion(FilesConnectionStringOptions server)
        {
            throw new NotImplementedException();
        }

        public void ShowProgress(string format, params object[] args)
        {
            if (Progress != null)
            {
                Progress(string.Format(format, args));
            }
        }


        public LastFilesEtagsInfo FetchCurrentMaxEtags()
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<SmugglerFileInfo>> GetFiles(FilesConnectionStringOptions src, Etag lastEtag, int take)
        {
            throw new NotImplementedException();
        }

        public Task PutFiles(Stream files, RavenJObject metadata)
        {
            throw new NotImplementedException();
        }
    }
}
