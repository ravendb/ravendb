// -----------------------------------------------------------------------
//  <copyright file="HttpTransportSender.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Rachis.Messages;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Sparrow;

namespace Rachis.Transport
{
    /// <summary>
    /// All requests are fire & forget, with the reply coming in (if at all)
    /// from the resulting thread.
    /// </summary>
    public class HttpTransportSender : IDisposable
    {
        private readonly HttpTransportBus _bus;

        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentDictionary<string, SecuredAuthenticator> _securedAuthenticatorCache = new ConcurrentDictionary<string, SecuredAuthenticator>();

        private readonly ConcurrentDictionary<HttpCacheKey, ConcurrentQueue<HttpClient>> _httpClientsCache = new ConcurrentDictionary<HttpCacheKey, ConcurrentQueue<HttpClient>>();

        private readonly ConcurrentDictionary<NodeConnectionInfo, int> _connectionFailureCounts = new ConcurrentDictionary<NodeConnectionInfo, int>();

        private readonly ILog _log;

        private readonly TimeSpan _shortOperationsTimeout;

        public HttpTransportSender(string name, TimeSpan shortOperationsTimeout, HttpTransportBus bus, CancellationToken cancellationToken)
        {
            _bus = bus;
            _cancellationToken = cancellationToken;
            _log = LogManager.GetLogger(GetType().Name + "." + name);
            _shortOperationsTimeout = shortOperationsTimeout;
        }

        public void Stream(NodeConnectionInfo dest, InstallSnapshotRequest req, Action<Stream> streamWriter)
        {
            LogStatus("install snapshot to " + dest, async () =>
            {
                var requestUri =
                    string.Format("raft/installSnapshot?term={0}&lastIncludedIndex={1}&lastIncludedTerm={2}&from={3}&topology={4}&clusterTopologyId={5}",
                        req.Term, req.LastIncludedIndex, req.LastIncludedTerm, req.From, Uri.EscapeDataString(JsonConvert.SerializeObject(req.Topology)), req.ClusterTopologyId);
                using (var request = CreateRequest(dest, null, requestUri, HttpMethods.Post, _log))
                {
                    var httpResponseMessage = await request.WriteAsync(() => new SnapshotContent(streamWriter)).ConfigureAwait(false);
                    UpdateConnectionFailureCounts(dest, httpResponseMessage);

                    var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (httpResponseMessage.IsSuccessStatusCode == false && httpResponseMessage.StatusCode != HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error installing snapshot to {0}. Status: {1}\r\n{2}", dest.Name, httpResponseMessage.StatusCode, reply);
                        return;
                    }
                    var installSnapshotResponse = JsonConvert.DeserializeObject<InstallSnapshotResponse>(reply);
                    SendToSelf(installSnapshotResponse);
                }
            });
        }

        public class SnapshotContent : HttpContent
        {
            private readonly Action<Stream> _streamWriter;
            private Stream _stream;

            public SnapshotContent(Action<Stream> streamWriter)
            {
                _streamWriter = streamWriter;
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _stream = stream;
                _streamWriter(stream);

                return Task.FromResult(1);
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }
            
            public override string ToString()
            {
                if (_stream != null && _stream.CanSeek)
                {
                    _stream.Seek(0, SeekOrigin.Begin);
                    byte[] buffer = new byte[16 * 1024];
                    using (MemoryStream ms = new MemoryStream())
                    {
                        int read;
                        while ((read = _stream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            ms.Write(buffer, 0, read);
                        }
                        return Convert.ToBase64String(ms.ToArray());
                    }
                }
                else
                {
                    return "SnapshotContent is either not set with a stream or the stream isn't seekable";
                }
            }
        }

        private HttpRaftRequest CreateRequest(NodeConnectionInfo node, TimeSpan? timeout, string url, HttpMethod httpMethod, ILog logger)
        {

            if (timeout.HasValue == false)
            {
                timeout = TimeSpan.FromSeconds(20); // use default if not defined
            }

            var request = new HttpRaftRequest(node, url, httpMethod, info =>
            {
                HttpClient client;
                var dispose = (IDisposable)GetConnection(info, timeout.Value, out client);
                return Tuple.Create(dispose, client);
            },
            _cancellationToken, logger)
            {
                UnauthorizedResponseAsyncHandler = HandleUnauthorizedResponseAsync,
                ForbiddenResponseAsyncHandler = HandleForbiddenResponseAsync
            };
            GetAuthenticator(node).ConfigureRequest(this, new WebRequestEventArgs
            {
                Client = request.HttpClient,
                Credentials = node.ToOperationCredentials()
            });
            return request;
        }

        internal async Task<Action<HttpClient>> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse, NodeConnectionInfo nodeConnectionInfo)
        {
            var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

            if (nodeConnectionInfo.ApiKey == null)
            {
                AssertUnauthorizedCredentialSupportWindowsAuth(unauthorizedResponse, nodeConnectionInfo);
                return null;
            }

            if (string.IsNullOrEmpty(oauthSource))
                oauthSource = nodeConnectionInfo.Uri.AbsoluteUri + "/OAuth/API-Key";

            return await GetAuthenticator(nodeConnectionInfo).DoOAuthRequestAsync(nodeConnectionInfo.Uri.AbsoluteUri, oauthSource, nodeConnectionInfo.ApiKey).ConfigureAwait(false);
        }

        private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response, NodeConnectionInfo nodeConnectionInfo)
        {
            if (nodeConnectionInfo.Username == null)
                return;

            var authHeaders = response.Headers.WwwAuthenticate.FirstOrDefault();
            if (authHeaders == null || (authHeaders.ToString().Contains("NTLM") == false && authHeaders.ToString().Contains("Negotiate") == false))
            {
                // we are trying to do windows auth, but we didn't get the windows auth headers
                throw new SecurityException(
                    "Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
                    + " but either wrong credentials where entered or the specified server does not support Windows authentication." +
                    Environment.NewLine +
                    "If you are running inside IIS, make sure to enable Windows authentication.");
            }
        }

        internal Task<Action<HttpClient>> HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse, NodeConnectionInfo nodeConnection)
        {
            if (nodeConnection.ApiKey == null)
            {
                AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse, nodeConnection);
                return null;
            }

            return null;
        }

        private void AssertForbiddenCredentialSupportWindowsAuth(HttpResponseMessage response, NodeConnectionInfo nodeConnection)
        {
            if (nodeConnection.ToOperationCredentials().Credentials == null)
                return;

            var requiredAuth = response.Headers.GetFirstValue("Raven-Required-Auth");
            if (requiredAuth == "Windows")
            {
                // we are trying to do windows auth, but we didn't get the windows auth headers
                throw new SecurityException(
                    "Attempted to connect to a RavenDB Server that requires authentication using Windows credentials, but the specified server does not support Windows authentication." +
                    Environment.NewLine +
                    "If you are running inside IIS, make sure to enable Windows authentication.");
            }
        }

        public void Send(NodeConnectionInfo dest, AppendEntriesRequest req)
        {
            if (MaybeIgnoreFrequentRequestsIfServerDown(dest,"append entries"))
                return;

            LogStatus("append entries to " + dest, async () =>
            {
                var requestUri = string.Format("raft/appendEntries?term={0}&leaderCommit={1}&prevLogTerm={2}&prevLogIndex={3}&entriesCount={4}&from={5}&clusterTopologyId={6}",
                    req.Term, req.LeaderCommit, req.PrevLogTerm, req.PrevLogIndex, req.EntriesCount, req.From, req.ClusterTopologyId);
                using (var request = CreateRequest(dest, _shortOperationsTimeout, requestUri, HttpMethods.Post, _log))
                {
                    var content = new EntriesContent(req.Entries);
                    var contentHash = Hashing.XXHash64.Calculate(await content.ReadAsStringAsync().ConfigureAwait(false), Encoding.UTF8);                    

                    using (var httpResponseMessage = await request.WriteAsync(() => content, "Request-Hash", contentHash.ToString()).ConfigureAwait(false))
                    {                                               
                        UpdateConnectionFailureCounts(dest, httpResponseMessage);

                        var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                        if (httpResponseMessage.IsSuccessStatusCode == false && httpResponseMessage.StatusCode != HttpStatusCode.NotAcceptable)
                        {
                            _log.Warn("Error appending entries to {0}. Status: {1}\r\n{2}\r\nreason:{3}", dest.Name, httpResponseMessage.StatusCode, reply, httpResponseMessage.ReasonPhrase);
                            return;
                        }

                        if (httpResponseMessage.StatusCode == HttpStatusCode.NotAcceptable)
                        {
                            _log.Warn("Error appending entries to {0}. Status: NotAcceptable\r\nreason:{1}\r\ncontent{2}", dest.Name, httpResponseMessage.ReasonPhrase, reply);
                        }

                        var appendEntriesResponse = JsonConvert.DeserializeObject<AppendEntriesResponse>(reply);
                        SendToSelf(appendEntriesResponse);
                    }
                }
            });
        }

        internal class EntriesContent : HttpContent
        {
            private readonly LogEntry[] _entries;

            public EntriesContent(LogEntry[] entries)
            {
                _entries = entries;
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                foreach (var logEntry in _entries)
                {
                    Write7BitEncodedInt64(stream, logEntry.Index);
                    Write7BitEncodedInt64(stream, logEntry.Term);
                    stream.WriteByte(logEntry.IsTopologyChange == true ? (byte)1 : (byte)0);
                    Write7BitEncodedInt64(stream, logEntry.Data.Length);
                    stream.Write(logEntry.Data, 0, logEntry.Data.Length);
                }
                return Task.FromResult(1);
            }

            private void Write7BitEncodedInt64(Stream stream, long value)
            {
                var v = (ulong)value;
                while (v >= 128)
                {
                    stream.WriteByte((byte)(v | 128));
                    v >>= 7;
                }
                stream.WriteByte((byte)(v));
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }

            public override string ToString()
            {
                return string.Join(", ", _entries.Select(x => x.ToString()));
            }
        }

        public void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req)
        {
            LogStatus("can install snapshot to " + dest, async () =>
            {
                var requestUri = string.Format("raft/canInstallSnapshot?term={0}&index={1}&from={2}&clusterTopologyId={3}", req.Term, req.Index,
                    req.From, req.ClusterTopologyId);
                using (var request = CreateRequest(dest, null, requestUri, HttpMethods.Get, _log))
                {
                    var httpResponseMessage = await request.ExecuteAsync().ConfigureAwait(false);
                    UpdateConnectionFailureCounts(dest, httpResponseMessage);

                    var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (httpResponseMessage.IsSuccessStatusCode == false && httpResponseMessage.StatusCode != HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error checking if can install snapshot to {0}. Status: {1}\r\n{2}\r\nreason:{3}", dest.Name, httpResponseMessage.StatusCode, reply,httpResponseMessage.ReasonPhrase);
                        return;
                    }
                    if (httpResponseMessage.StatusCode == HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error checking if can install snapshot to {0}. Status: NotAcceptable\r\nreason:{1}\r\ncontent{2}", dest.Name, httpResponseMessage.ReasonPhrase, reply);
                    }
                    var canInstallSnapshotResponse = JsonConvert.DeserializeObject<CanInstallSnapshotResponse>(reply);
                    SendToSelf(canInstallSnapshotResponse);
                }
            });
        }

        public void Send(NodeConnectionInfo dest, RequestVoteRequest req)
        {
            if (MaybeIgnoreFrequentRequestsIfServerDown(dest,"vote"))
                return;

            LogStatus("request vote from " + dest, async () =>
            {
                var requestUri = string.Format("raft/requestVote?term={0}&lastLogIndex={1}&lastLogTerm={2}&trialOnly={3}&forcedElection={4}&from={5}&clusterTopologyId={6}",
                    req.Term, req.LastLogIndex, req.LastLogTerm, req.TrialOnly, req.ForcedElection, req.From, req.ClusterTopologyId);
                using (var request = CreateRequest(dest, _shortOperationsTimeout, requestUri, HttpMethods.Get, _log))
                {
                    var httpResponseMessage = await request.ExecuteAsync().ConfigureAwait(false);
                    UpdateConnectionFailureCounts(dest, httpResponseMessage);

                    var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (httpResponseMessage.IsSuccessStatusCode == false && httpResponseMessage.StatusCode != HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error requesting vote from {0}. Status: {1}\r\n{2}\r\nreason:{3}", dest.Name, httpResponseMessage.StatusCode, reply, httpResponseMessage.ReasonPhrase);
                        return;
                    }
                    if (httpResponseMessage.StatusCode == HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error requesting vote from {0}. Status: NotAcceptable\r\nreason:{1}\r\ncontent{2}", dest.Name, httpResponseMessage.ReasonPhrase, reply);
                    }
                    var requestVoteResponse = JsonConvert.DeserializeObject<RequestVoteResponse>(reply);
                    SendToSelf(requestVoteResponse);
                }
            });
        }

        private void SendToSelf(object o)
        {
            _bus.Publish(o, source: null);
        }

        public void Send(NodeConnectionInfo dest, TimeoutNowRequest req)
        {
            LogStatus("timeout to " + dest, async () =>
            {
                var requestUri = string.Format("raft/timeoutNow?term={0}&from={1}&clusterTopologyId={2}", req.Term, req.From, req.ClusterTopologyId);
                using (var request = CreateRequest(dest, null, requestUri, HttpMethods.Get, _log))
                {
                    var httpResponseMessage = await request.ExecuteAsync().ConfigureAwait(false);
                    UpdateConnectionFailureCounts(dest, httpResponseMessage);

                    var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (httpResponseMessage.IsSuccessStatusCode == false)
                    {
                        _log.Warn("Error sending timeout now to {0}. Status: {1}\r\n{2}", dest.Name, httpResponseMessage.StatusCode, httpResponseMessage, reply);
                        return;
                    }
                    if (httpResponseMessage.StatusCode == HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error sending timeout now to {0}. Status: NotAcceptable\r\nreason:{1}\r\ncontent{2}", dest.Name, httpResponseMessage.ReasonPhrase, reply);
                    }
                    SendToSelf(new NothingToDo());
                }
            });
        }

        public void Send(NodeConnectionInfo dest, DisconnectedFromCluster req)
        {
            LogStatus("disconnect " + dest, async () =>
            {
                var requestUri = string.Format("raft/disconnectFromCluster?term={0}&from={1}&clusterTopologyId={2}", req.Term, req.From, req.ClusterTopologyId);
                using (var request = CreateRequest(dest, null, requestUri, HttpMethods.Get, _log))
                {
                    var httpResponseMessage = await request.ExecuteAsync().ConfigureAwait(false);
                    UpdateConnectionFailureCounts(dest, httpResponseMessage);

                    var reply = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (httpResponseMessage.IsSuccessStatusCode == false)
                    {
                        _log.Warn("Error sending disconnecton notification to {0}. Status: {1}\r\n{2}\r\nreason:{3}", dest.Name, httpResponseMessage.StatusCode, httpResponseMessage, reply,httpResponseMessage.ReasonPhrase);
                        return;
                    }
                    if (httpResponseMessage.StatusCode == HttpStatusCode.NotAcceptable)
                    {
                        _log.Warn("Error sending disconnecton notification to {0}. Status: NotAcceptable\r\nreason:{1}\r\ncontent{2}", dest.Name, httpResponseMessage.ReasonPhrase, reply);
                    }

                    SendToSelf(new NothingToDo());
                }
            });
        }

        private readonly ConcurrentDictionary<Task, object> _runningOps = new ConcurrentDictionary<Task, object>();

        private void LogStatus(string details, Func<Task> operation)
        {
            var op = operation();
            _runningOps.TryAdd(op, op);
            op
                .ContinueWith(task =>
                {
                    object value;
                    _runningOps.TryRemove(op, out value);
                    if (task.Exception != null)
                    {
                        _log.Warn("Failed to send " + details + " " + InnerMostMessage(task.Exception), task.Exception);
                        return;
                    }
                    _log.Info("Sent {0}", details);
                });
        }

        private string InnerMostMessage(Exception exception)
        {
            if (exception.InnerException == null)
                return exception.Message;
            return InnerMostMessage(exception.InnerException);
        }


        public void Dispose()
        {
            foreach (var q in _httpClientsCache.Select(x => x.Value))
            {
                HttpClient result;
                while (q.TryDequeue(out result))
                {
                    result.Dispose();
                }
            }
            _httpClientsCache.Clear();

            _securedAuthenticatorCache.Clear();

            var array = _runningOps.Keys.ToArray();
            _runningOps.Clear();
            try
            {
                Task.WaitAll(array);
            }
            catch (Exception)
            {
                // nothing to do here, we just need to wait until 
                // all the requests are done, we don't care if they errored
                // the error handling is actually handled in the LogStatus method
                // but we need to wait so we won't have an old request hitting a new server in tests
            }
        }


        internal SecuredAuthenticator GetAuthenticator(NodeConnectionInfo info)
        {
            return _securedAuthenticatorCache.GetOrAdd(info.Name, _ => new SecuredAuthenticator(autoRefreshToken: false));
        }

        internal ReturnToQueue GetConnection(NodeConnectionInfo nodeConnection, TimeSpan timeout, out HttpClient result)
        {
            var cacheKey = new HttpCacheKey(nodeConnection, timeout);
            var connectionQueue = _httpClientsCache.GetOrAdd(cacheKey, _ => new ConcurrentQueue<HttpClient>());

            if (connectionQueue.TryDequeue(out result) == false)
            {
                var webRequestHandler = new WebRequestHandler
                {
                    UseDefaultCredentials = nodeConnection.HasCredentials() == false,
                    Credentials = nodeConnection.ToOperationCredentials().Credentials
                };

                result = new HttpClient(webRequestHandler)
                {
                    BaseAddress = nodeConnection.Uri,
                    Timeout = timeout
                };
            }

            return new ReturnToQueue(result, connectionQueue);
        }

        internal class HttpCacheKey
        {
            private NodeConnectionInfo NodeConnection { get; set; }
            private TimeSpan Timeout { get; set; }

            public HttpCacheKey(NodeConnectionInfo nodeConnection, TimeSpan timeout)
            {
                NodeConnection = nodeConnection;
                Timeout = timeout;
            }

            protected bool Equals(HttpCacheKey other)
            {
                return Equals(NodeConnection, other.NodeConnection) && Timeout.Equals(other.Timeout);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                {
                    return false;
                }
                if (ReferenceEquals(this, obj))
                {
                    return true;
                }
                if (obj.GetType() != this.GetType())
                {
                    return false;
                }
                return Equals((HttpCacheKey) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((NodeConnection != null ? NodeConnection.GetHashCode() : 0)*397) ^ Timeout.GetHashCode();
                }
            }
        }

        internal struct ReturnToQueue : IDisposable
        {
            private readonly HttpClient client;
            private readonly ConcurrentQueue<HttpClient> queue;

            public ReturnToQueue(HttpClient client, ConcurrentQueue<HttpClient> queue)
            {
                this.client = client;
                this.queue = queue;
            }

            public void Dispose()
            {
                queue.Enqueue(client);
            }
        }

        private bool MaybeIgnoreFrequentRequestsIfServerDown(NodeConnectionInfo node, string whichRequest)
        {
            var ignore = false;
            _connectionFailureCounts.AddOrUpdate(node, 0, (_, connectionFailureCount) =>
            {
                if (connectionFailureCount <= 20)
                    return connectionFailureCount;

                if (connectionFailureCount % 20 == 0)
                    return connectionFailureCount;

                ignore = true;
                return connectionFailureCount + 1;
            });
            if (_log.IsDebugEnabled && ignore == true)
            {
                _log.Debug($"Will not send {whichRequest} request to {node.Uri} because it is failing too frequently");
            }            
            return ignore;
        }

        private void UpdateConnectionFailureCounts(NodeConnectionInfo node, HttpResponseMessage response)
        {
            _connectionFailureCounts.AddOrUpdate(node, 0, (_, failureCount) =>
            {
                if (response.IsSuccessStatusCode)
                    return 0;

                bool timeout;
                if (IsServerDown(response.StatusCode, out timeout))
                    return failureCount + 1;

                return 0;
            });
        }

        private static bool IsServerDown(HttpStatusCode httpStatusCode, out bool timeout)
        {
            timeout = false;
            switch (httpStatusCode)
            {
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.GatewayTimeout:
                    timeout = true;
                    return true;
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    return true;
            }
            return false;
        }
    }
}
