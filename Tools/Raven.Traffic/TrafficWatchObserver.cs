using System;
using System.Threading;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Traffic
{
    public class TrafficWatchObserver : IObserver<string>
    {
        private readonly IDocumentStore store;
        private string databaseName;
        private ManualResetEvent _mre;
        private DateTime _lastHeartbeat;
        private Action<RavenJObject> _onRequestReceived;
        private readonly DocumentConvention _conventions;
        private Timer timeoutTimer;


        public TrafficWatchObserver(IDocumentStore store, string databaseName, ManualResetEvent mre, TimeSpan timeout, Action<RavenJObject> onRequestReceived)
        {
            this.store = store;
            this.databaseName = databaseName;
            _mre = mre;
            _onRequestReceived = onRequestReceived;

            if (timeout != TimeSpan.MinValue && mre.WaitOne(0) == false)
            {
                timeoutTimer = new Timer(x =>
                {
                    if (SystemTime.UtcNow - _lastHeartbeat > timeout)
                    {
                        Console.WriteLine("Timeout Reached");
                        mre.Set();
                        return;
                    }
                    timeoutTimer.Change(timeout, TimeSpan.FromDays(7));

                }, null, timeout, TimeSpan.FromDays(7));
            }
        }

        public void OnNext(string dataFromConnection)
        {
            try
            {
                _lastHeartbeat = SystemTime.UtcNow;

                var ravenJObject = RavenJObject.Parse(dataFromConnection);
                var type = ravenJObject.Value<string>("Type");

                switch (type)
                {
                    case "Disconnect":
                        EstablishConnection();
                        break;
                    case "Initialized":
                    case "Heartbeat":
                        break;
                    default:
                        var value = ravenJObject.Value<RavenJObject>("Value");
                        _onRequestReceived(value);
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        public void OnError(Exception error)
        {
            DisposeSubscriptionAndRequest();
            EstablishConnection();
        }

        public void OnCompleted()
        {
            DisposeSubscriptionAndRequest();
            _mre.Set();
        }

        private void DisposeSubscriptionAndRequest()
        {
            if (currentSubscription != null)
            {
                currentSubscription.Dispose();
                currentSubscription = null;
            }
            if (currentRequest != null)
            {
                currentRequest.Dispose();
                currentRequest = null;
            }
        }

        /// <summary>
        /// Subscribes received ovserver to the given request, intended to be used for EventSource connections
        /// </summary>
        /// <param name="request">EventSource request</param>
        /// <param name="observer">Observer that will treat events</param>
        private void SubscribeToServerEvents(HttpJsonRequest request, IObserver<string> observer)
        {
            var serverPullTask = request.ServerPullAsync();
            serverPullTask.Wait();
            var serverEvents = serverPullTask.Result;
            currentSubscription = serverEvents.Subscribe(observer);
        }

        private HttpJsonRequest currentRequest;
        private IDisposable currentSubscription;

        public void EstablishConnection()
        {
            currentRequest = GetTrafficWatchRequest();

            SubscribeToServerEvents(currentRequest,
                this);
            
        }
        
        private string GetAuthToken()
        {
            string authToken;
            var databasePartialUrl = databaseName == null ? string.Empty : "//databases//" + databaseName;
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + databasePartialUrl + "/singleAuthToken",
                    "GET", store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions)))
            {
                authToken = request.ReadResponseJson().Value<string>("Token");
            }
            return authToken;
        }

        private HttpJsonRequest GetTrafficWatchRequest()
        {
            var databasePartialUrl = databaseName == null ? string.Empty : "//databases//" + databaseName;
            return store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + databasePartialUrl + "/traffic-watch/events?" + "singleUseAuthToken=" + GetAuthToken() + "&id=" + Guid.NewGuid(),
                    "GET",
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions)
                {
                    AvoidCachingRequest = true,
                    DisableRequestCompression = true
                });
        }
    }
}
