using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Smuggler.Database
{
    public interface IDatabaseSmugglerIdentityActions : IDisposable
    {
        Task WriteIdentityAsync(string name, long value, CancellationToken cancellationToken);
    }
}
