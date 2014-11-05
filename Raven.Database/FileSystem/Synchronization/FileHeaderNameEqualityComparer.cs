using System.Collections.Generic;
using Raven.Database.FileSystem.Storage;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization
{
    internal class FileHeaderNameEqualityComparer : IEqualityComparer<FileHeader>
	{
        public bool Equals(FileHeader x, FileHeader y)
		{
            return x.FullPath == y.FullPath;
		}

        public int GetHashCode(FileHeader header)
		{
            return (header.FullPath != null ? header.FullPath.GetHashCode() : 0);
		}
	}
}