namespace Raven.Client.Documents.Subscriptions
{
    /// <summary>
    /// Response wrapper for listing subscriptions.
    /// </summary>
    public sealed class GetSubscriptionsResult
    {
        /// <summary>Array of subscription states returned by the server.</summary>
        public SubscriptionState[] Results;
    }
}