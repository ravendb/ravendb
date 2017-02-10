using System;

namespace Raven.Client.Data
{
    public class DatabaseOperationsStatus
    {
        public DateTime? LastBackup { get; set; }

        public DateTime? LastAlertIssued { get; set; }
    }
}
