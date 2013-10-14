using System;
using System.Collections.Generic;
using System.Linq;
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
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Tasks
{
    public class RestoreDatabaseTask : DatabaseTask
    {
        private readonly string backupLocation;
        private readonly string databaseLocation;
        private readonly bool defrag;

        public RestoreDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, string backupLocation, string databaseLocation, bool defrag) : base(databaseCommands, "Restore Database", databaseName)
        {
            this.backupLocation = backupLocation;
            this.databaseLocation = databaseLocation;
            this.defrag = defrag;
        }

	    private int reportedMessageCount = 0;

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            await DatabaseCommands.ForSystemDatabase().DeleteDocumentAsync("Raven/Restore/Status");

            var failCount = 0;
            await DatabaseCommands.Admin.StartRestoreAsync(backupLocation, databaseLocation, DatabaseName);

            var restoreFinished = false;
            reportedMessageCount = 0;

            while (!restoreFinished)
            {
                await TaskEx.Delay(TimeSpan.FromSeconds(1));

                var doc = await DatabaseCommands.ForSystemDatabase().GetAsync("Raven/Restore/Status");
                if (doc == null)
                {
                    if (failCount >= 5)
                    {
                        ReportError(
                            "Could not find restore status document, can not know if errors have accrued or if process was completed");
                        return DatabaseTaskOutcome.Error;
                    }
                    else
                    {
                        failCount++;
                    }
                }
                else
                {
                    var statusMessages = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();
                    foreach (var statusMessage in statusMessages.Skip(reportedMessageCount))
                    {
                        Report(statusMessage);
                        reportedMessageCount++;
                    }

                    restoreFinished =
                        statusMessages.Last().Contains("The new database was created") ||
						statusMessages.Last().Contains("Restore Canceled") ||
						statusMessages.Last().Contains("A database name must be supplied if the restore location does not contain a valid Database.Document file") ||
						statusMessages.Last().Contains("Cannot do an online restore for the <system> database") ||
						statusMessages.Last().Contains("Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name");
                }
            }

            return DatabaseTaskOutcome.Succesful;
        }

	    public override void OnError()
	    {
		    DatabaseCommands.ForSystemDatabase().GetAsync("Raven/Restore/Status").ContinueOnSuccessInTheUIThread(
			    doc =>
			    {
				    if (doc == null)
					    return;

				    var statusMessages = doc.DataAsJson["restoreStatus"].Values().Select(token => token.ToString()).ToList();
				    foreach (var statusMessage in statusMessages.Skip(reportedMessageCount))
				    {
					    Report(statusMessage);
					    reportedMessageCount++;
				    }
			    });
	    }
    }
}
