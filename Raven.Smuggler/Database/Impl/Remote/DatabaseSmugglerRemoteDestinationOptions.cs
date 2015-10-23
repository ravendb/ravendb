using System;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDestinationOptions
	{
		private const int DefaultDocumentSizeInChunkLimitInBytes = 8 * 1024 * 1024;

		private long _totalDocumentSizeInChunkLimitInBytes;

		private int _chunkSize;

		public DatabaseSmugglerRemoteDestinationOptions()
		{
			TotalDocumentSizeInChunkLimitInBytes = DefaultDocumentSizeInChunkLimitInBytes;
			ChunkSize = int.MaxValue;
			HeartbeatLatency = TimeSpan.FromSeconds(10);
		}

		public TimeSpan HeartbeatLatency { get; set; }

		public string ContinuationToken { get; set; }

		public bool WaitForIndexing { get; set; }

		public long TotalDocumentSizeInChunkLimitInBytes
		{
			get { return _totalDocumentSizeInChunkLimitInBytes; }
			set
			{
				if (value < 1024)
					throw new InvalidOperationException("Total document size in a chunk cannot be less than 1kb");

				_totalDocumentSizeInChunkLimitInBytes = value;
			}
		}

		public bool DisableCompression { get; set; }

		public int ChunkSize
		{
			get { return _chunkSize; }
			set
			{
				if (value < 1)
					throw new InvalidOperationException("Chunk size cannot be zero or a negative number");
				_chunkSize = value;
			}
		}
	}
}