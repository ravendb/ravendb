using Raven.Abstractions.Replication;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class AddReplicationCommand : Command
	{
		private readonly BaseSettingsModel settingsModel;

		public AddReplicationCommand(BaseSettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			settingsModel.ReplicationDestinations.Add(new ReplicationDestination());
		}
	}
}
