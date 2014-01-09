using System;
using Raven.Abstractions.Data;
#if !SILVERLIGHT
using Raven.Client.Connection;
using Raven.Client.Shard;
#endif

namespace Raven.Client.Document.Batches
{
	public interface ILazyOperation
	{
		GetRequest CreateRequest();
		object Result { get; }
		QueryResult QueryResult { get; }
		bool RequiresRetry { get; }
		void HandleResponse(GetResponse response);
#if !SILVERLIGHT
		void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy);
#endif

		IDisposable EnterContext();
#if !SILVERLIGHT
		object ExecuteEmbedded(IDatabaseCommands commands);
		void HandleEmbeddedResponse(object result);
#endif
	}
}