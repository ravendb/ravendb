using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal sealed class PagingPerformanceDetails : INotificationDetails
    {
        public PagingPerformanceDetails()
        {
            Actions = new Dictionary<string, Queue<ActionDetails>>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, Queue<ActionDetails>> Actions { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var key in Actions.Keys)
            {
                var queue = Actions[key];
                if (queue == null)
                    continue;

                var list = new DynamicJsonArray();
                foreach (var details in queue)
                {
                    list.Add(new DynamicJsonValue
                    {
                        [nameof(ActionDetails.NumberOfResults)] = details.NumberOfResults,
                        [nameof(ActionDetails.PageSize)] = details.PageSize,
                        [nameof(ActionDetails.Occurrence)] = details.Occurrence,
                        [nameof(ActionDetails.Duration)] = details.Duration,
                        [nameof(ActionDetails.Details)] = details.Details,
                        [nameof(ActionDetails.TotalDocumentsSizeInBytes)] = details.TotalDocumentsSizeInBytes
                    });
                }

                djv[key] = list;
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Actions)] = djv
            };
        }

        public void Update(Paging.PagingInformation pagingInfo)
        {
            if (Actions.TryGetValue(pagingInfo.Action, out Queue<ActionDetails> actionDetails) == false)
                Actions[pagingInfo.Action] = actionDetails = new Queue<ActionDetails>();

            actionDetails.Enqueue(new ActionDetails
            {
                Duration = pagingInfo.Duration,
                Occurrence = pagingInfo.Occurrence,
                NumberOfResults = pagingInfo.NumberOfResults,
                PageSize = pagingInfo.PageSize,
                Details = pagingInfo.Details,
                TotalDocumentsSizeInBytes = pagingInfo.TotalDocumentsSizeInBytes
            });

            while (actionDetails.Count > 10)
                actionDetails.Dequeue();
        }

        internal sealed class ActionDetails
        {
            public string Details { get; set; }
            public long Duration { get; set; }
            public DateTime Occurrence { get; set; }
            public long NumberOfResults { get; set; }
            public long PageSize { get; set; }
            public long TotalDocumentsSizeInBytes { get; set; }
        }
    }
}
