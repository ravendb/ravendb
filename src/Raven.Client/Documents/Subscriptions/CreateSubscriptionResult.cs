namespace Raven.Client.Documents.Subscriptions
{
    public class CreateSubscriptionResult
    {
        public string Name { get; set; }
        public long RaftCommandIndex { get; set; }
    }

    public sealed class UpdateSubscriptionResult : CreateSubscriptionResult
    {
    }
}
