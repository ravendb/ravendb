using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public class WebSocketsConfiguration
    {
        [DefaultValue(128)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("WebSockets.InitialBufferPoolSizeInKb")]
        public Size InitialBufferPoolSize { get; set; }
    }
}