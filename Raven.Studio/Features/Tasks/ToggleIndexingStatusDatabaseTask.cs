using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;

namespace Raven.Studio.Features.Tasks
{
    public enum ToggleIndexAction
    {
        TurnOff,
        TurnOn
    }

    public class ToggleIndexingStatusDatabaseTask : DatabaseTask
    {
        private readonly ToggleIndexAction action;

        public ToggleIndexingStatusDatabaseTask(ToggleIndexAction action, IAsyncDatabaseCommands databaseCommands, string databaseName)
            : base(databaseCommands, "Toggle Indexing", databaseName)
        {
            this.action = action;
        }

        protected async override Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (action == ToggleIndexAction.TurnOff)
            {
                Report("Disabling Indexing ... (will wait for current indexing batch to complete)");

                await DatabaseCommands.Admin.StopIndexingAsync();

                Report("Indexing Disabled");
            }
            else
            {
                Report("Enabling Indexing");

                await DatabaseCommands.Admin.StartIndexingAsync();

                Report("Indexing Enabled");
            }

            return DatabaseTaskOutcome.Succesful;
        }

		public override void OnError()
		{
			
		}
    }
}
