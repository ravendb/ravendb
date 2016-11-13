using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Counters
{
    public class CounterBackupRequest
    {
        /// <summary>
        /// Path to directory where backup should lie (must be accessible from server).
        /// </summary>
        public string BackupLocation { get; set; }

        /// <summary>
        /// A counter that will be backed up. If null then document will be taken from server.
        /// </summary>
        public CounterStorageDocument CounterDocument { get; set; }
    }
}
