using System;

namespace Voron
{
	using System.IO;

	public class ReadResult : IDisposable
	{
		public ReadResult(Stream stream, ushort version)
		{
			Stream = stream;
			Version = Version;
		}

		public Stream Stream { get; private set; }

		public ushort Version { get; private set; }
		public void Dispose()
		{
			Stream.Dispose();
		}
	}
}