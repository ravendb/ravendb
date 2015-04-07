using System;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.FileSystem
{
	public class SynchronizationReport
	{
		public SynchronizationReport()
		{
		}

		[JsonConstructor]
		public SynchronizationReport(string fileName, Guid fileETag, SynchronizationType type)
		{
			FileName = fileName;
			FileETag = fileETag;
			Type = type;
		}

		public string FileName { get; private set; }
		public Etag FileETag { get; private set; }
		public long BytesTransfered { get; set; }
		public long BytesCopied { get; set; }
		public long NeedListLength { get; set; }
		public Exception Exception { get; set; }
		public SynchronizationType Type { get; private set; }
	}
}