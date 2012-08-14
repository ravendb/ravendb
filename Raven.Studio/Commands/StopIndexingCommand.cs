using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class StopIndexingCommand : Command
	{
		private readonly Action<string> output;
		private readonly Action afterAction;

		public StopIndexingCommand(Action<string> output, Action afterAction)
		{
			this.output = output;
			this.afterAction = afterAction;
		}

		public override void Execute(object parameter)
		{
			ShouldExecute = false;
			output("Disabling indexing... (will wait for current indexing batch to complete)");
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.StopIndexingAsync()
				.ContinueOnSuccessInTheUIThread(() =>
				                                	{
				                                		output("Indexing disabled");
				                                		afterAction();
				                                	});
		}
	}
}