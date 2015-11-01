using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Owin;

namespace Raven.Monitor
{
    using WebSocketAccept = Action<IDictionary<string, object>, // options
    Func<IDictionary<string, object>, Task>>; // callback
    using WebSocketCloseAsync =
        Func<int /* closeStatus */,
            string /* closeDescription */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveAsync =
        Func<ArraySegment<byte> /* data */,
            CancellationToken /* cancel */,
            Task<Tuple<int /* messageType */,
                bool /* endOfMessage */,
                int /* count */>>>;
    using WebSocketSendAsync =
        Func<ArraySegment<byte> /* data */,
            int /* messageType */,
            bool /* endOfMessage */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveResult = Tuple<int, // type
        bool, // end of message?
        int>; // count
    public class MonitorHost
    {
        public void Configuration(IAppBuilder appBuilder)
        {
            var config = new HttpConfiguration();
            config.MapHttpAttributeRoutes();
            config.Formatters.Remove(config.Formatters.XmlFormatter);
            config.Formatters.JsonFormatter.SerializerSettings.Converters
                .Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            appBuilder.UseWebApi(config);
        }
    }
}
