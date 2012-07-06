using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.SignalR.Client.Http;

namespace Raven.Imports.SignalR.Client
{
    public interface IConnection
    {
        string MessageId { get; set; }
        IEnumerable<string> Groups { get; set; }
        IDictionary<string, object> Items { get; }
        string ConnectionId { get; }
        string Url { get; }
        string QueryString { get; }
        ConnectionState State { get; }

        bool ChangeState(ConnectionState oldState, ConnectionState newState);

        ICredentials Credentials { get; set; }
        CookieContainer CookieContainer { get; set; }

        void Stop();
        Task Send(string data);
        Task<T> Send<T>(string data);

        void OnReceived(JToken data);
        void OnError(Exception ex);
        void OnReconnected();
        void PrepareRequest(IRequest request);
    }
}
