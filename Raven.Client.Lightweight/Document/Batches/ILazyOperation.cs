using System;
using Raven.Abstractions.Data;

namespace Raven.Client.Document.Batches
{
	public interface ILazyOperation
	{
		GetRequest CraeteRequest();
		object Result { get; set; }
		void HandleResponse(GetResponse response);
		IDisposable EnterContext();
	}
}