// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConfig
    {
        public long SubscriptionId { get; set; }
        public SubscriptionCriteria Criteria { get; set; }
        public Etag AckEtag { get; set; }
        public DateTime TimeOfSendingLastBatch { get; set; }
        public DateTime TimeOfLastAcknowledgment { get; set; }
        public DateTime TimeOfLastClientActivity { get; set; }
    }
}
