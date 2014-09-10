using System.Collections.Generic;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.Server.RavenFS.Synchronization
{
    internal class FileHeaderNameEqualityComparer : IEqualityComparer<FileHeader>
	{
        public bool Equals(FileHeader x, FileHeader y)
		{
            return x.FullName == y.FullName;
		}

        public int GetHashCode(FileHeader header)
		{
            return (header.FullName != null ? header.FullName.GetHashCode() : 0);
		}
	}
}