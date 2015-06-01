using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public interface IBulkInsertOperation
	{
		Guid OperationId { get; }

		bool IsAborted { get;}

		void Abort();

		event Action<string> Report;

		Task DisposeAsync();
			
		void Dispose();

		string Store(object entity);


		void Store(object entity, string id);

		void Store(RavenJObject document, RavenJObject metadata, string id, int? dataSize = null);

	}
}
