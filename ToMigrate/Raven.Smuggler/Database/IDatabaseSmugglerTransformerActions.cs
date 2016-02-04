using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;

namespace Raven.Smuggler.Database
{
    public interface IDatabaseSmugglerTransformerActions : IDisposable
    {
        Task WriteTransformerAsync(TransformerDefinition transformer, CancellationToken cancellationToken);
    }
}
