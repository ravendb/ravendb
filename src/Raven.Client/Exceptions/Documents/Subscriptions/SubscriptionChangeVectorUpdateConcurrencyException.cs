using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionChangeVectorUpdateConcurrencyException : SubscriptionException
    {
        public SubscriptionChangeVectorUpdateConcurrencyException(string message) : base(message)
        {
        }      
    }
}
