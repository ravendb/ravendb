using Raven.Client.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
	public struct ReplicationBatchItem
	{
		public LazyStringValue Key;
		public ChangeVectorEntry[] ChangeVector;
		public BlittableJsonReaderObject Data;
		public long Etag;
		public LazyStringValue Collection;
	}
}