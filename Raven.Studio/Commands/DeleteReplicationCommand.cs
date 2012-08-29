using Raven.Abstractions.Replication;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteReplicationCommand : Command
	{
		private readonly BaseSettingsModel settingsModel;

		public DeleteReplicationCommand(BaseSettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			var destination = parameter as ReplicationDestination;
			if (destination == null)
				return;
			settingsModel.ReplicationDestinations.Remove(destination);
			settingsModel.SelectedReplication = null;
		}
	}
}