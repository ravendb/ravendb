using Raven.Abstractions.Replication;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteReplicationCommand : Command
	{
		private readonly BaseBundlesModel bundlesModel;

		public DeleteReplicationCommand(BaseBundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			var destination = parameter as ReplicationDestination;
			if (destination == null)
				return;
			bundlesModel.ReplicationDestinations.Remove(destination);
			bundlesModel.SelectedReplication = null;
		}
	}
}