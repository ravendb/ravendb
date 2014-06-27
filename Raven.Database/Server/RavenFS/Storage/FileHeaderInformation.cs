using System;
using System.Collections.Specialized;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;
using System.IO;

namespace Raven.Database.Server.RavenFS.Storage
{
	public class FileHeaderInformation 
	{
		public string Name { get; set; }
		public long? TotalSize { get; set; }
		public long UploadedSize { get; set; }

		public string HumaneTotalSize
		{
			get { return Humane(TotalSize); }
		}

		public string HumaneUploadedSize
		{
			get { return Humane(UploadedSize); }
		}

        public DateTimeOffset LastModified 
        {
            get 
            {
                var lastModified = new DateTimeOffset();
                if (this.Metadata.Keys.Contains("Last-Modified")) 
                {
                    lastModified = this.Metadata["Last-Modified"].Value<DateTimeOffset>(); 
                }
                return lastModified;
            }
        }

        public DateTimeOffset CreationDate
        {
            get {
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
                if (this.Metadata.Keys.Contains("ETag"))
                {
                    Etag.TryParse(this.Metadata["ETag"].Value<string>(), out parsedEtag);
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

		public RavenJObject Metadata { get; set; }

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

		protected bool Equals(FileHeaderInformation other)
		{
			return string.Equals(Name, other.Name) && TotalSize == other.TotalSize && UploadedSize == other.UploadedSize && Equals(Metadata, other.Metadata);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((FileHeaderInformation)obj);
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