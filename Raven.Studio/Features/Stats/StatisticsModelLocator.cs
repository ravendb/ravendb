using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Stats
{
	public class StatisticsModelLocator : ModelLocatorBase<ServerModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<ServerModel> observable)
		{
			observable.Value = ServerModel;
		}
	}
}