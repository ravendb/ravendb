using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesRestoreRequest
    {
        public string Id { get; set; }

        public string BackupLocation { get; set; }

        public string RestoreToLocation { get; set; }
    }
}
