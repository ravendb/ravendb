using System;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class NextBackup
    {
        public TimeSpan TimeSpan { get; set; }

        public bool IsFull { get; set; }
    }
}