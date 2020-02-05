using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;
using Redbus.Events;

namespace Tests.ResourceSnapshotAggregator
{
    public class BuildNotification : EventBase
    {
        public string JobName { get; set; }

        public string BuildNumber { get; set; }

        public string JobUrl { get; set; }

        public string BuildUrl { get; set; }

        public string GitUrl { get; set; }

        public string GitBranch { get; set; }

        public string GitCommitHash { get; set; }
        
        [JsonIgnore]
        public DateTime When { get; } = DateTime.UtcNow;

        public IEnumerable<string> CheckMissingFields()
        {
            //those are needed to retrieve artifacts

            if (string.IsNullOrEmpty(JobName))
                yield return nameof(JobName);

            if (string.IsNullOrEmpty(BuildNumber))
                yield return nameof(BuildNumber);
        }
    }
}
