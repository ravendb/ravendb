using Raven.Abstractions.Replication;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class AddReplicationCommand : Command
	{
		private readonly BaseBundlesModel bundlesModel;

		public AddReplicationCommand(BaseBundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			bundlesModel.ReplicationDestinations.Add(new ReplicationDestination());
		}
	}
}
