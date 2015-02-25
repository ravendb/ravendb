//// -----------------------------------------------------------------------
////  <copyright file="RaftController.cs" company="Hibernating Rhinos LTD">
////      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
////  </copyright>
//// -----------------------------------------------------------------------

//using System.Diagnostics;
//using System.IO;
//using System.Net.Http;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Web.Http;
//using System.Web.Http.Controllers;
//using Newtonsoft.Json;
//using Rachis.Messages;
//using Rachis.Storage;

//namespace Rachis.Transport
//{
//	public class RaftController : ApiController
//	{
//		private HttpTransportBus _bus;

//		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
//		{
//			_bus = (HttpTransportBus) controllerContext.Configuration.Properties[typeof (HttpTransportBus)];
//			var sp = Stopwatch.StartNew();
//			var msg = await base.ExecuteAsync(controllerContext, cancellationToken);
//			if (_bus.Log.IsDebugEnabled)
//			{
//				_bus.Log.Debug("{0} {1} {2} in {3:#,#;;0} ms", msg.StatusCode, controllerContext.Request.Method, controllerContext.Request.RequestUri, 
//					sp.ElapsedMilliseconds);
//			}
//			return msg;
//		}


//		[HttpPost]
//		[Route("raft/installSnapshot")]
//		public async Task<HttpResponseMessage> InstallSnapshot([FromUri]InstallSnapshotRequest request, [FromUri]string topology)
//		{
//			request.Topology = JsonConvert.DeserializeObject<Topology>(topology);
//			var stream = await Request.Content.ReadAsStreamAsync();
//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource, stream);
//			return await taskCompletionSource.Task;
//		}

//		[HttpPost]
//		[Route("raft/appendEntries")]
//		public async Task<HttpResponseMessage> AppendEntries([FromUri]AppendEntriesRequest request, [FromUri]int entriesCount)
//		{
//			var stream = await Request.Content.ReadAsStreamAsync();
//			request.Entries = new LogEntry[entriesCount];
//			for (int i = 0; i < entriesCount; i++)
//			{
//				var index = Read7BitEncodedInt(stream);
//				var term = Read7BitEncodedInt(stream);
//				var isTopologyChange = stream.ReadByte() == 1;
//				var lengthOfData = (int)Read7BitEncodedInt(stream);
//				request.Entries[i] = new LogEntry
//				{
//					Index = index,
//					Term = term,
//					IsTopologyChange = isTopologyChange,
//					Data = new byte[lengthOfData]
//				};

//				var start = 0;
//				while (start < lengthOfData)
//				{
//					var read = stream.Read(request.Entries[i].Data, start, lengthOfData - start);
//					start += read;
//				}
//			}

//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource);
//			return await taskCompletionSource.Task;
//		}

//		internal protected long Read7BitEncodedInt(Stream stream)
//		{
//			long count = 0;
//			int shift = 0;
//			byte b;
//			do
//			{
//				if (shift == 9 * 7)
//					throw new InvalidDataException("Invalid 7bit shifted value, used more than 9 bytes");

//				var maybeEof = stream.ReadByte();
//				if (maybeEof == -1)
//					throw new EndOfStreamException();

//				b = (byte)maybeEof;
//				count |= (uint)(b & 0x7F) << shift;
//				shift += 7;
//			} while ((b & 0x80) != 0);
//			return count;
//		}

//		[HttpGet]
//		[Route("raft/requestVote")]
//		public Task<HttpResponseMessage> RequestVote([FromUri]RequestVoteRequest request)
//		{
//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource);
//			return taskCompletionSource.Task;
//		}

//		[HttpGet]
//		[Route("raft/timeoutNow")]
//		public Task<HttpResponseMessage> TimeoutNow([FromUri]TimeoutNowRequest request)
//		{
//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource);
//			return taskCompletionSource.Task;
//		}

//		[HttpGet]
//		[Route("raft/disconnectFromCluster")]
//		public Task<HttpResponseMessage> DisconnectFromCluster([FromUri]DisconnectedFromCluster request)
//		{
//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource);
//			return taskCompletionSource.Task;
//		}

//		[HttpGet]
//		[Route("raft/canInstallSnapshot")]
//		public Task<HttpResponseMessage> CanInstallSnapshot([FromUri]CanInstallSnapshotRequest request)
//		{
//			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
//			_bus.Publish(request, taskCompletionSource);
//			return taskCompletionSource.Task;
//		}
//	}
//}