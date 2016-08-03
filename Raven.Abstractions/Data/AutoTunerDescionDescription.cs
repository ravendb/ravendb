using System;

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