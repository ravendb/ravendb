using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Data
{
    public class RestoreInProgress
    {
        public const string RavenRestoreInProgressDocumentKey = "Raven/Restore/InProgress";

        public string Resource { get; set; }

    }
}
