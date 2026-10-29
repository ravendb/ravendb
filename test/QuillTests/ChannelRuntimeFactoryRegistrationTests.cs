using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelRuntimeFactoryRegistrationTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Every_channel_runtime_factory_is_registered()
    {
        var declared = typeof(IChannelRuntimeFactory).Assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IChannelRuntimeFactory).IsAssignableFrom(t))
            .Select(t => t.Name)
            .Order()
            .ToArray();

        var registered = Host.Services.GetServices<IChannelRuntimeFactory>()
            .Select(f => f.GetType().Name)
            .Order()
            .ToArray();

        Assert.NotEmpty(declared);
        Assert.Equal(declared, registered);
    }
}
