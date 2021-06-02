using System;
using Sparrow.Json.Parsing;
using System.Collections.Generic;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class WarnIndexOutputsPerDocument : INotificationDetails
    {
        public Dictionary<string, Queue<WarningDetails>> Warnings { get; set; }
        
        public WarnIndexOutputsPerDocument()
        {
            Warnings = new Dictionary<string, Queue<WarningDetails>>(StringComparer.OrdinalIgnoreCase);
        }
        
        public void Update(string indexName, WarningDetails warning)
        {
            if (Warnings.TryGetValue(indexName, out Queue<WarningDetails> warningDetails) == false)
                Warnings[indexName] = warningDetails = new Queue<WarningDetails>();

            warningDetails.Enqueue(warning);

            while (warningDetails.Count > 10)
                warningDetails.Dequeue();
        }
        
        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var key in Warnings.Keys)
            {
                var queue = Warnings[key];
                if (queue == null)
                    continue;

                var list = new DynamicJsonArray();
                foreach (var details in queue)
                {
                    list.Add(new DynamicJsonValue
                    {
                        [nameof(WarningDetails.NumberOfExceedingDocuments)] = details.NumberOfExceedingDocuments,
                        [nameof(WarningDetails.SampleDocumentId)] = details.SampleDocumentId,
                        [nameof(WarningDetails.MaxNumberOutputsPerDocument)] = details.MaxNumberOutputsPerDocument,
                        [nameof(WarningDetails.Suggestion)] = details.Suggestion,
                        [nameof(WarningDetails.LastWarningTime)] = details.LastWarningTime
                    });
                }

                djv[key] = list;
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Warnings)] = djv
            };
        }
        
        public class WarningDetails
        {
            public long NumberOfExceedingDocuments { get; set; }
            public string SampleDocumentId { get; set; }
            public int MaxNumberOutputsPerDocument { get; set; }
            public string Suggestion { get; set; }
            public DateTime? LastWarningTime { get; set; }
        }
    }
}