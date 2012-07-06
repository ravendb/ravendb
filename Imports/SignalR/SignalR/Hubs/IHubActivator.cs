namespace Raven.Imports.SignalR.Hubs
{
    public interface IHubActivator
    {
        IHub Create(HubDescriptor descriptor);
    }
}