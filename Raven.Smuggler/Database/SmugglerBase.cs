using System.Threading.Tasks;

using Raven.Abstractions.Smuggler.Data;

namespace Raven.Smuggler.Database
{
	internal abstract class SmugglerBase
	{
		protected readonly DatabaseSmugglerOptions Options;

		protected readonly ReportActions Report;

		protected readonly IDatabaseSmugglerSource Source;

		protected readonly IDatabaseSmugglerDestination Destination;

		protected SmugglerBase(DatabaseSmugglerOptions options, ReportActions report, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
		{
			Options = options;
			Report = report;
			Source = source;
			Destination = destination;
		}

		public abstract Task SmuggleAsync(OperationState state);
	}
}