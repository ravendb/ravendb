using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesBackupRequest
    {
        /// <summary>
        /// Path to directory where backup should lie (must be accessible from server).
        /// </summary>
        public string BackupLocation { get; set; }

        /// <summary>
        /// A time series that will be backed up. If null then document will be taken from server.
        /// </summary>
        public TimeSeriesDocument TimeSeriesDocument { get; set; }
    }
}
