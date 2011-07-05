#if !NET_3_5
using System;
using Raven.Abstractions.Data;

namespace Raven.Client.Document.Batches
{
	public interface ILazyOperation
	{
		GetRequest CraeteRequest();
		object Result { get;  }
		bool RequiresRetry { get; }
		void HandleResponse(GetResponse response);
		IDisposable EnterContext();
	}
}
#endif