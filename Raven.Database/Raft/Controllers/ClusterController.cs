// -----------------------------------------------------------------------
//  <copyright file="ClusterController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Dto;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;
using Sparrow;

namespace Raven.Database.Raft.Controllers
{
    public class ClusterController : BaseDatabaseApiController
    {
        private HttpTransport Transport
        {
            get
            {
                return (HttpTransport)ClusterManager.Engine.Transport;
            }
        }

        private HttpTransportBus Bus
        {
            get
            {
                return Transport.Bus;
            }
        }


        [HttpGet]
        [RavenRoute("cluster/status")]
        public HttpResponseMessage Status()
        {
            var allNodes = ClusterManager.Engine.CurrentTopology.AllNodes.ToList();
            var tasks = allNodes.Select(x => 
                new
                {
                    Uri = x.Uri.AbsoluteUri, 
                    Task = FetchNodeStatus(x)
                }).ToArray();
            return GetMessageWithObject(tasks.Select(x => new { Uri = x.Uri, Status = x.Task.Result }).ToList());
        }

        private Task<Tuple<ConnectivityStatus,string>> FetchNodeStatus(NodeConnectionInfo nci)
        {
            return ClusterManager.Client.CheckConnectivity(nci);
        }

        [HttpGet]
        [RavenRoute("cluster/topology")]
        public HttpResponseMessage Topology()
        {
            return GetMessageWithObject(ClusterManager?.GetTopology());
        }

        [HttpPost]
        [RavenRoute("raft/installSnapshot")]
        public async Task<HttpResponseMessage> InstallSnapshot([FromUri]InstallSnapshotRequest request, [FromUri]string topology)
        {
            request.Topology = JsonConvert.DeserializeObject<Topology>(topology);
            var stream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource, stream);
            return await taskCompletionSource.Task.ConfigureAwait(false);
        }

        [HttpPost]
        [RavenRoute("raft/appendEntries")]
        public async Task<HttpResponseMessage> AppendEntries()
        {
            int version;
            if (!TryGetParamFromUri("version", out version) || version != 2)
            {
                return GetMessageWithString("Received append entries from incorrect version of RavenDB instance. Are all cluster nodes updated with the same assembly version? Url received: " + Request.RequestUri, HttpStatusCode.BadRequest);
            }

            var hashAsString = GetHeader("Request-Hash");
            var bodyContentLengthAsString = GetHeader("Request-Length");
            ulong receivedHash = 0;
            if (hashAsString != null && !ulong.TryParse(hashAsString, out receivedHash))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Expected for 'Request-Hash' header key to exist, and it should be a ulong. Received : " + hashAsString);
                }
                return GetMessageWithString("Expected for 'Request-Hash' header key to exist, and it should be a ulong. Received : " + hashAsString, HttpStatusCode.BadRequest);
            }

            int bodyContentLength = 0;
            if (bodyContentLengthAsString != null && !int.TryParse(bodyContentLengthAsString, out bodyContentLength))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Expected for 'Request-Length' header key to exist, and it should be a int. Received : " + bodyContentLengthAsString);
                }
                return GetMessageWithString("Expected for 'Request-Length' header key to exist, and it should be a int. Received : " + bodyContentLengthAsString, HttpStatusCode.BadRequest);
            }

            AppendEntriesRequest request;
            int entriesCount;
            HttpResponseMessage failureResponseMessage;
            if (!TryParseAppendEntriesRequestFromUri(out request, out entriesCount, out failureResponseMessage))
                return failureResponseMessage;

            using (var contentStream = new MemoryStream())
            {
                var sourceStream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false);
                await sourceStream.CopyToAsync(contentStream).ConfigureAwait(false);
                HttpResponseMessage responseMessage;
                if (VerifyContentHash(contentStream, 
                        receivedHash, 
                        bodyContentLength, 
                        out responseMessage) == false)
                    return responseMessage;

                contentStream.Position = 0;

                for (int i = 0; i < entriesCount; i++)
                {
                    request.Entries[i] = LogEntry.ReadFromStream(contentStream);
                }

                var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
                Bus.Publish(request, taskCompletionSource);
                return await taskCompletionSource.Task.ConfigureAwait(false);
            }
        }

        private bool VerifyContentHash(Stream stream, ulong receivedHash, int bodyContentLength, out HttpResponseMessage responseMessage)
        {
            stream.Position = 0;
            responseMessage = null;

            //not disposing the reader because we want to keep the 'stream' open
            var reader = new BinaryReader(stream);
            var contentArray = reader.ReadBytes(bodyContentLength);
            if(contentArray.Length < bodyContentLength)
                throw new EndOfStreamException("Failed to verify content hash because we reached the end of the request body stream.");

            var hash = Hashing.XXHash64.Calculate(contentArray);
            if (hash != receivedHash)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Received content hash is not equal to received hash headers. This means something tampered with HTTP request contents... Received hash " + receivedHash + ", but expected " + hash);
                }

                responseMessage = GetMessageWithString("Received content hash is not equal to received hash headers. This means something tampered with HTTP request contents...", HttpStatusCode.BadRequest);
                return false;
            }

            return true;
        }

        private bool TryParseAppendEntriesRequestFromUri(out AppendEntriesRequest request, out int entriesCount, out HttpResponseMessage responseMessage)
        {
            request = new AppendEntriesRequest();
            responseMessage = null;
            entriesCount = 0;

            if (!TryGetParamFromUri("entriesCount", out entriesCount))
            {
                responseMessage = GetMessageWithString("EntriesCount parameter is not a number! Integer number expected.", HttpStatusCode.BadRequest);
                return false;
            }

            request.Entries = new LogEntry[entriesCount];

            long term;
            if (!TryGetParamFromUri("term", out term))
            {
                responseMessage = GetMessageWithString("Term parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.Term = term;

            long leaderCommit;
            if (!TryGetParamFromUri("leaderCommit", out leaderCommit))
            {
                responseMessage = GetMessageWithString("leaderCommit parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.LeaderCommit = leaderCommit;

            long prevLogTerm;
            if (!TryGetParamFromUri("prevLogTerm", out prevLogTerm))
            {
                responseMessage = GetMessageWithString("prevLogTerm parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.PrevLogTerm = prevLogTerm;

            long prevLogIndex;
            if (!TryGetParamFromUri("prevLogIndex", out prevLogIndex))
            {
                responseMessage = GetMessageWithString("prevLogIndex parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.PrevLogIndex = prevLogIndex;

            Guid clusterTopologyId;
            if (!TryGetParamFromUri("clusterTopologyId", out clusterTopologyId))
            {
                responseMessage = GetMessageWithString("clusterTopologyId parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.ClusterTopologyId = clusterTopologyId;

            string from;
            if (!TryGetParamFromUri("from", out from))
            {
                responseMessage = GetMessageWithString("from parameter is not a number! Long number expected.", HttpStatusCode.BadRequest);
                return false;
            }
            request.From = from;

            return true;
        }

        private bool TryGetParamFromUri<TParam>(string name, out TParam value)
        {
            value = default(TParam);
            var valueAsString = GetQueryStringValue(name);
            
            object conversionResult;
            var type = typeof(TParam);

            if (type == typeof(Guid))
            {
                Guid guid;
                if (Guid.TryParse(valueAsString, out guid))
                {
                    value = (TParam)Convert.ChangeType(guid, type);
                    return true;
                }
                return false;
            }

            try
            {
                conversionResult = Convert.ChangeType(valueAsString, type);
            }
            catch (InvalidCastException e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.ErrorException("Failed to convert " + name + " query parameter to type " + type, e);
                }
                return false;
            }
            catch (FormatException e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.ErrorException(name + " query parameter is not a recognized format", e);
                }
                return false;
            }
            catch (OverflowException e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.ErrorException(name + " query parameter is a number that is out of range for " + type, e);
                }
                return false;
            }

            if (conversionResult != null)
            {
                value = (TParam) conversionResult;
                return true;
            }

            return false;
        }

        [HttpGet]
        [RavenRoute("raft/requestVote")]
        public Task<HttpResponseMessage> RequestVote([FromUri]RequestVoteRequest request)
        {
            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource);
            return taskCompletionSource.Task;
        }

        [HttpGet]
        [RavenRoute("raft/timeoutNow")]
        public Task<HttpResponseMessage> TimeoutNow([FromUri]TimeoutNowRequest request)
        {
            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource);
            return taskCompletionSource.Task;
        }

        [HttpGet]
        [RavenRoute("raft/disconnectFromCluster")]
        public Task<HttpResponseMessage> DisconnectFromCluster([FromUri]DisconnectedFromCluster request)
        {
            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource);
            return taskCompletionSource.Task;
        }

        [HttpGet]
        [RavenRoute("raft/canInstallSnapshot")]
        public Task<HttpResponseMessage> CanInstallSnapshot([FromUri]CanInstallSnapshotRequest request)
        {
            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource);
            return taskCompletionSource.Task;
        }


        [HttpPost]
        [RavenRoute("cluster/replicationState")]
        public async Task<HttpResponseMessage> ReplicationState()
        {
            var databaseToLastModify = await ReadJsonObjectAsync<ReplicationState>().ConfigureAwait(false);
            await ClusterManager.Client.SendReplicationStateAsync(databaseToLastModify).ConfigureAwait(false);
            return GetEmptyMessage();
        }

      
    }
}
