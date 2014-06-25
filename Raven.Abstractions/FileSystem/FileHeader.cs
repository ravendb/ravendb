using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class FileHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Name { get; set; }
        public long? TotalSize { get; set; }

        public string Path { get; private set; }
        public string Extension { get; private set; }

        public DateTimeOffset CreationDate { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public Etag Etag { get; private set; }

        public FileHeader( string name, RavenJObject metadata )
        {
            this.Extension = System.IO.Path.GetExtension(name);
            this.Path = System.IO.Path.GetDirectoryName(name);
            this.Name = System.IO.Path.GetFileName(name);
            this.Metadata = metadata;
            
            if (this.TotalSize <= 0 || metadata.Keys.Contains("RavenFS-Size"))
            {
                this.TotalSize = metadata["RavenFS-Size"].Value<long>();
            }
            
            Etag parsedEtag = new Etag();
            if (metadata.Keys.Contains("ETag"))
            {
                if (Etag.TryParse(metadata["ETag"].Value<string>(), out parsedEtag))
                    this.Etag = parsedEtag;
            }

            this.LastModified = new DateTimeOffset();
            if (metadata.Keys.Contains("Last-Modified"))
            {
                this.LastModified = metadata["Last-Modified"].Value<DateTimeOffset>();
            }

            this.CreationDate = new DateTimeOffset();
            if (metadata.Keys.Contains("Creation-Date"))
            {
                this.CreationDate = metadata["Creation-Date"].Value<DateTimeOffset>();
            }
        }

        protected FileHeader()
        {

        }

        public string HumaneTotalSize
        {
            get 
            {
                return Humane(this.TotalSize);
            }
        }


        public static string Humane(long? size)
        {
            if (size == null)
                return null;

            var absSize = Math.Abs(size.Value);
            const double GB = 1024 * 1024 * 1024;
            const double MB = 1024 * 1024;
            const double KB = 1024;

            if (absSize > GB) // GB
                return string.Format("{0:#,#.##} GBytes", size / GB);
            if (absSize > MB)
                return string.Format("{0:#,#.##} MBytes", size / MB);
            if (absSize > KB)
                return string.Format("{0:#,#.##} KBytes", size / KB);
            return string.Format("{0:#,#} Bytes", size);
        }
    }

}
