using System.Collections.Generic;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	internal class FileHeaderNameEqualityComparer : IEqualityComparer<FileHeaderInformation>
	{
		public bool Equals(FileHeaderInformation x, FileHeaderInformation y)
		{
			return x.Name == y.Name;
		}

		public int GetHashCode(FileHeaderInformation header)
		{
			return (header.Name != null ? header.Name.GetHashCode() : 0);
		}
	}
}