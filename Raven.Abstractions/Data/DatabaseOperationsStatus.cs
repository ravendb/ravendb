using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Data
{
    public class DatabaseOperationsStatus
    {
        public DateTime? LastBackup { get; set; }
        public DateTime? LastAlertIssued { get; set; }
    }
}
