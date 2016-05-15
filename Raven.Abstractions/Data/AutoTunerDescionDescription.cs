using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Raven.Abstractions.Data
{
    public class AutoTunerDecisionDescription
    {
        public AutoTunerDecisionDescription(string name, string dbname, string reason)
        {
            Time = SystemTime.UtcNow;
            Reason = reason;
            Name = name;
            DatabaseName = dbname;
        }
     
        public DateTime Time { get; set; }
        public string Name { get; set; }
        public string DatabaseName { get; set; }
        public string Reason { get; set; }

    }
}