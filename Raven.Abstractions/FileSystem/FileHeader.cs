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
        public RavenJObject Metadata { get; set; }

        public string Name { get; set; }

        private long? _totalSize;
        public long? TotalSize
        {
            get
            {
                if (!_totalSize.HasValue)
                {
                    SetFileSize();
                }

                return _totalSize;
            }
            set 
            {
                this._totalSize = value; 
            }
        }
        public long UploadedSize { get; set; }

        public DateTimeOffset LastModified
        {
            get
            {
                var lastModified = new DateTimeOffset();
                if (this.Metadata.Keys.Contains(Constants.LastModified))
                {
                    lastModified = this.Metadata[Constants.LastModified].Value<DateTimeOffset>();
                }
                return lastModified;
            }
        }

        public DateTimeOffset CreationDate
        {
            get
            {
                var creationDate = new DateTimeOffset();
                if (this.Metadata.Keys.Contains("Creation-Date"))
                {
                    creationDate = this.Metadata["Creation-Date"].Value<DateTimeOffset>();
                }
                return creationDate;
            }
        }

        public Etag Etag
        {
            get
            {
                Etag parsedEtag = null;
                if (this.Metadata.Keys.Contains(Constants.MetadataEtagField))
                {
                    Etag.TryParse(this.Metadata[Constants.MetadataEtagField].Value<string>(), out parsedEtag);
                }

                return parsedEtag;
            }
        }

        public string Extension
        {
            get
            {
                return System.IO.Path.GetExtension(this.Name);
            }
        }

        public string Path
        {
            get
            {
                return System.IO.Path.GetDirectoryName(this.Name);
            }
        }

        public FileHeader( string name, RavenJObject metadata )
        {
            this.Name = name;
            this.Metadata = metadata;
            SetFileSize();
        }

        private void SetFileSize()
        {
            if (!this._totalSize.HasValue && this.Metadata.Keys.Contains("RavenFS-Size"))
            {
                var metadataTotalSize = this.Metadata["RavenFS-Size"].Value<long>();
                if (metadataTotalSize > 0)
                    this._totalSize = metadataTotalSize;
            }
            
            if (this.UploadedSize <= 0)
                this.UploadedSize = this._totalSize.HasValue ? this._totalSize.Value : 0;

        }

        public FileHeader()
        {}

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

        public bool IsFileBeingUploadedOrUploadHasBeenBroken()
        {
            return TotalSize == null || TotalSize != UploadedSize || (Metadata[SynchronizationConstants.RavenDeleteMarker] == null && Metadata["Content-MD5"] == null);
        }

        protected bool Equals(FileHeader other)
        {
            return string.Equals(Name, other.Name) && TotalSize == other.TotalSize && UploadedSize == other.UploadedSize && Metadata.Equals(other.Metadata);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((FileHeader)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ TotalSize.GetHashCode();
                hashCode = (hashCode * 397) ^ UploadedSize.GetHashCode();
                hashCode = (hashCode * 397) ^ (Metadata != null ? Metadata.GetHashCode() : 0);
                return hashCode;
            }
        }
    }

}
