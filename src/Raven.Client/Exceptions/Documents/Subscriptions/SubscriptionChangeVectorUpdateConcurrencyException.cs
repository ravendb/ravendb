namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public sealed class SubscriptionChangeVectorUpdateConcurrencyException : SubscriptionException
    {
        public SubscriptionChangeVectorUpdateConcurrencyException(string message) : base(message)
        {
        }      
    }
}
