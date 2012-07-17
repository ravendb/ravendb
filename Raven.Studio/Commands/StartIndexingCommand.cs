using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class StartIndexingCommand : Command
	{
		private readonly Action<string> output;

		public StartIndexingCommand(Action<string> output)
		{
			this.output = output;
		}

		public override void Execute(object parameter)
		{
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.StartIndexing()
				.ContinueOnSuccessInTheUIThread(() => output("Indexing enabled"));
		}
	}
}