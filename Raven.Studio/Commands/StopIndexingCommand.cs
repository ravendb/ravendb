using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class StopIndexingCommand : Command
	{
		private readonly Action<string> output;

		public StopIndexingCommand(Action<string> output)
		{
			this.output = output;
		}

		public override void Execute(object parameter)
		{
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.StopIndexing()
				.ContinueOnSuccessInTheUIThread(() => output("Indexing disabled"));
		}
	}
}