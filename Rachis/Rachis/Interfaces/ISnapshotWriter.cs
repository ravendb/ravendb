using System;
using System.IO;

namespace Rachis.Interfaces
{
	public interface ISnapshotWriter : IDisposable
	{
		long Index { get; 	}
		long Term { get; }
		void WriteSnapshot(Stream stream);
	}
}