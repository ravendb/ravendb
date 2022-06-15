namespace Raven.Server.Documents.Subscriptions;

public interface ISubscriptionSemaphore
{
    public bool TryEnterSubscriptionsSemaphore();

    public void ReleaseSubscriptionsSemaphore();
}
