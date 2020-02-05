using Nancy;
using Nancy.TinyIoc;
using Redbus.Interfaces;

namespace Tests.ResourceSnapshotAggregator
{
    public class NancyBootstrapper : DefaultNancyBootstrapper
    {
        private readonly IEventBus _messageBus;

        public NancyBootstrapper(IEventBus messageBus) => _messageBus = messageBus;

        protected override void ConfigureApplicationContainer(TinyIoCContainer container)
        {
            container.Register(_messageBus);
            base.ConfigureApplicationContainer(container);
        }
    }
}
