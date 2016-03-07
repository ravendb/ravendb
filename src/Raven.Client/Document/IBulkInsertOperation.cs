using System;
using System.Threading.Tasks;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public interface IBulkInsertOperation : IDisposable
    {
        Guid OperationId { get; }

        bool IsAborted { get; }

        Task WriteAsync(string id, RavenJObject metadata, RavenJObject data);

        Task DisposeAsync();

        /// <summary>
        ///     Report on the progress of the operation
        /// </summary>
        event Action<string> Report;
        void Abort();
    }
}