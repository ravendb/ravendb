namespace Raven.Client.Documents.Subscriptions
{
    public class CreateSubscriptionResult
    {
        public string Name { get; set; }
    }

    public class UpdateSubscriptionResult : CreateSubscriptionResult
    {
    }
}
