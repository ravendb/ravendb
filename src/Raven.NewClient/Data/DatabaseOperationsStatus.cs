using System;

namespace Raven.NewClient.Abstractions.Data
{
    public class DatabaseOperationsStatus
    {
        public DateTime? LastBackup { get; set; }

        public DateTime? LastAlertIssued { get; set; }
    }
}
