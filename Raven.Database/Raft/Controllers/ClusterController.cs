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
            var hashAsString = GetHeader("Request-Hash");
            ulong receivedHash = 0;
            if (hashAsString != null && !ulong.TryParse(hashAsString, out receivedHash))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Expected for 'Request-Hash' header key to exist, and it should be a ulong. Received : " + hashAsString);
                }
                return GetMessageWithString("Expected for 'Request-Hash' header key to exist, and it should be a ulong. Received : " + hashAsString, HttpStatusCode.BadRequest);
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
                if (VerifyContentHash(contentStream, receivedHash, out responseMessage) == false)
                    return responseMessage;

                contentStream.Position = 0;
                
                for (int i = 0; i < entriesCount; i++)
                {
                    var index = Read7BitEncodedInt(contentStream);
                    var term = Read7BitEncodedInt(contentStream);
                    var isTopologyChange = contentStream.ReadByte() == 1;
                    var lengthOfData = (int) Read7BitEncodedInt(contentStream);
                    request.Entries[i] = new LogEntry
                    {
                        Index = index,
                        Term = term,
                        IsTopologyChange = isTopologyChange,
                        Data = new byte[lengthOfData]
                    };

                    var start = 0;
                    while (start < lengthOfData)
                    {
                        var read = contentStream.Read(request.Entries[i].Data, start, lengthOfData - start);
                        start += read;
                    }
                }

                var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
                Bus.Publish(request, taskCompletionSource);
                return await taskCompletionSource.Task.ConfigureAwait(false);
            }
        }

        private bool VerifyContentHash(Stream stream, ulong receivedHash, out HttpResponseMessage responseMessage)
        {
            stream.Position = 0;
            responseMessage = null;
            using (var sr = new StreamReader(stream, Encoding.UTF8,true,1024,true))
            {
                var contentAsString = sr.ReadToEnd();
                var hash = Hashing.XXHash64.Calculate(contentAsString, Encoding.UTF8);
                
                if (hash != receivedHash)
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug("Received content hash is not equal to received hash headers. This means something tampered with HTTP request contents... Received hash " + receivedHash + ", but expected " + hash);
                    }
                    responseMessage = GetMessageWithString("Received content hash is not equal to received hash headers. This means something tampered with HTTP request contents...", HttpStatusCode.BadRequest);
                    return false;
                }
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

        private static long Read7BitEncodedInt(Stream stream)
        {
            long count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 9 * 7)
                    throw new InvalidDataException("Invalid 7bit shifted value, used more than 9 bytes");

                var maybeEof = stream.ReadByte();
                if (maybeEof == -1)
                    throw new EndOfStreamException();

                b = (byte)maybeEof;
                count |= (uint)(b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }
    }
}
