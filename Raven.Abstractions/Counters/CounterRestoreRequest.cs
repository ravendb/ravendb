using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Counters
{
    public class CounterRestoreRequest
    {
        public string Id { get; set; }

        public string BackupLocation { get; set; }

        public string RestoreToLocation { get; set; }
    }
}
