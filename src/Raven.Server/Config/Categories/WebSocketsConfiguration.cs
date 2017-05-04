using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public class WebSocketsConfiguration
    {
        [DefaultValue(128 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Raven/WebSockets/InitialBufferPoolSize")]
        public Size InitialBufferPoolSize { get; set; }
    }
}