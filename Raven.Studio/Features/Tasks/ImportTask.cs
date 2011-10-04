using System.Windows.Input;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class ImportTask : TaskModel
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

        public ImportTask(IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            Name = "Import Database";
            Description = "Import a database from a dump file.\nImporting will overwrite any existing indexes.";
            
        }

        public class ImportDatabaseCommand : Command
        {
            private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

            public ImportDatabaseCommand(IAsyncDatabaseCommands asyncDatabaseCommands)
            {
                this.asyncDatabaseCommands = asyncDatabaseCommands;
            }

            public override void Execute(object parameter)
            {
                throw new System.NotImplementedException();
            }
        }

        public override ICommand Action
        {
            get { return new ImportDatabaseCommand(asyncDatabaseCommands); }
        }
    }
}
