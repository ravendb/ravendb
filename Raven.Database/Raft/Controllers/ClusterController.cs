// -----------------------------------------------------------------------
//  <copyright file="ClusterController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Dto;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;

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

        private Task<ConnectivityStatus> FetchNodeStatus(NodeConnectionInfo nci)
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
        public async Task<HttpResponseMessage> AppendEntries([FromUri]AppendEntriesRequest request, [FromUri]int entriesCount)
        {
            var stream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false);
            request.Entries = new LogEntry[entriesCount];
            for (int i = 0; i < entriesCount; i++)
            {
                var index = Read7BitEncodedInt(stream);
                var term = Read7BitEncodedInt(stream);
                var isTopologyChange = stream.ReadByte() == 1;
                var lengthOfData = (int)Read7BitEncodedInt(stream);
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
                    var read = stream.Read(request.Entries[i].Data, start, lengthOfData - start);
                    start += read;
                }
            }

            var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
            Bus.Publish(request, taskCompletionSource);
            return await taskCompletionSource.Task.ConfigureAwait(false);
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
