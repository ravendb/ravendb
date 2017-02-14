// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionCriteria
    {
        public string Collection { get; set; }
        public string FilterJavaScript { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        public string Collection { get; set; }
        public string FilterJavaScript { get; set; }
    }
}