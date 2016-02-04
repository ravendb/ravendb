using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class WebSocketsConfiguration
    {
        [DefaultValue(128 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Raven/WebSockets/InitialBufferPoolSize")]
        public Size InitialBufferPoolSize { get; set; }
    }
}