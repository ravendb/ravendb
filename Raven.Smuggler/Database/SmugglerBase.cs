using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal abstract class SmugglerBase
	{
		protected readonly DatabaseSmugglerOptions Options;

		protected readonly Report Report;

		protected readonly IDatabaseSmugglerSource Source;

		protected readonly IDatabaseSmugglerDestination Destination;

		protected SmugglerBase(DatabaseSmugglerOptions options, Report report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
		{
			Options = options;
			Report = report;
			Source = source;
			Destination = destination;
		}

		public abstract Task SmuggleAsync(OperationState state, CancellationToken cancellationToken);
	}
}