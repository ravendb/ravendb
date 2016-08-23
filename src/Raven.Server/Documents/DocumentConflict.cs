using Raven.Client.Replication.Messages;
using Sparrow.Json;

namespace Raven.Server.Documents
{
	public class DocumentConflict
	{
		public LazyStringValue Key;
		public BlittableJsonReaderObject Doc;
		public long StorageId;
		public ChangeVectorEntry[] ChangeVector;
	}
}