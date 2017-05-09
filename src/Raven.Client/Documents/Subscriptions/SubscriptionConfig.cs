// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionConfig : IFillFromBlittableJson
    {
        public long SubscriptionId { get; set; }
        public SubscriptionCriteria Criteria { get; set; }
        public long? AckEtag { get; set; }
        public DateTime TimeOfSendingLastBatch { get; set; }
        public DateTime TimeOfLastClientActivity { get; set; }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            if (json == null)
                return;

            long subscriptionId;
            if (json.TryGet(nameof(SubscriptionId), out subscriptionId))
                SubscriptionId = subscriptionId;

            long ackEtag;
            if (json.TryGet(nameof(AckEtag), out ackEtag))
                AckEtag = ackEtag;

            DateTime timeOfSendingLastBatch;
            if (json.TryGet(nameof(TimeOfSendingLastBatch), out timeOfSendingLastBatch))
                TimeOfSendingLastBatch = timeOfSendingLastBatch;

            DateTime timeOfLastClientActivity;
            if (json.TryGet(nameof(TimeOfLastClientActivity), out timeOfLastClientActivity))
                TimeOfLastClientActivity = timeOfLastClientActivity;

            BlittableJsonReaderObject criteria;
            if (json.TryGet(nameof(Criteria), out criteria))
            {
                Criteria = new SubscriptionCriteria(Constants.Documents.Collections.AllDocumentsCollection);
                Criteria.FillFromBlittableJson(criteria);
            } 
        }
    }
}