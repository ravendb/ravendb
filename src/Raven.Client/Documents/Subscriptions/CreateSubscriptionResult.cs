namespace Raven.Client.Documents.Subscriptions
{
    /// <summary>
    /// Result returned after creating a subscription.
    /// </summary>
    public class CreateSubscriptionResult
    {
        /// <summary>The created subscription name.</summary>
        public string Name { get; set; }
        /// <summary>The Raft command index for the operation that created/updated the subscription.</summary>
        public long RaftCommandIndex { get; set; }
    }

    /// <summary>
    /// Result returned after updating a subscription.
    /// Inherits common fields from <see cref="CreateSubscriptionResult"/>.
    /// </summary>
    public sealed class UpdateSubscriptionResult : CreateSubscriptionResult
    {
    }
}
