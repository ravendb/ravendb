using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Json.Linq;

namespace Raven.Smuggler.Database
{
    public interface IDatabaseSmugglerDocumentActions : IDisposable
    {
        Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken);
    }
}
