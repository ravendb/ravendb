#if !NET_3_5
using System;
using Raven.Abstractions.Data;
#if !SILVERLIGHT
using Raven.Client.Shard;
#endif

namespace Raven.Client.Document.Batches
{
	public interface ILazyOperation
	{
		GetRequest CraeteRequest();
		object Result { get;  }
		bool RequiresRetry { get; }
		void HandleResponse(GetResponse response);
#if !SILVERLIGHT
		void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy);
#endif

		IDisposable EnterContext();
	}
}
#endif