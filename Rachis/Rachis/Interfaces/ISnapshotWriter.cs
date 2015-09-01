using System.IO;

namespace Rachis.Interfaces
{
	public interface ISnapshotWriter
	{
		long Index { get; 	}
		long Term { get; }
		void WriteSnapshot(Stream stream);
	}
}