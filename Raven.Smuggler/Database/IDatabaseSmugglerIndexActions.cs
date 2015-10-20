using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerIndexActions : IDisposable
	{
		Task WriteIndexAsync(IndexDefinition index, CancellationToken cancellationToken);
	}
}