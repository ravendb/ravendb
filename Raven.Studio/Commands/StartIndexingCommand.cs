using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class StartIndexingCommand : Command
	{
		private readonly Action<string> output;
		private readonly Action afterAction;

		public StartIndexingCommand(Action<string> output, Action afterAction)
		{
			this.output = output;
			this.afterAction = afterAction;
		}

		public override void Execute(object parameter)
		{
			ShouldExecute = false;
			output("Enabling indexing...");
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.StartIndexingAsync()
				.ContinueOnSuccessInTheUIThread(() =>
				                                	{
				                                		output("Indexing enabled");
				                                		afterAction();
				                                	});
		}
	}
}