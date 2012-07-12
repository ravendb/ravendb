using System.Collections.Generic;

namespace Raven.Imports.SignalR.Hubs
{
    public class HubRequest
    {
        public string Hub { get; set; }
        public string Method { get; set; }
        public IJsonValue[] ParameterValues { get; set; }
        public IDictionary<string, object> State { get; set; }
        public string Id { get; set; }
    }
}
