using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.RavenFS;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization.Multipart
{
	public class SynchronizationMultipartRequest : IHoldProfilingInformation
	{
        private readonly RavenFileSystemClient.SynchronizationClient destination;
		private readonly string fileName;
		private readonly IList<RdcNeed> needList;
		private readonly ServerInfo serverInfo;
        private readonly RavenJObject sourceMetadata;
		private readonly Stream sourceStream;
		private readonly string syncingBoundary;
		private HttpJsonRequest request;

        public SynchronizationMultipartRequest(RavenFileSystemClient.SynchronizationClient destination, ServerInfo serverInfo, string fileName,
                                               RavenJObject sourceMetadata, Stream sourceStream, IList<RdcNeed> needList)
		{
			this.destination = destination;
			this.serverInfo = serverInfo;
			this.fileName = fileName;
			this.sourceMetadata = sourceMetadata;
			this.sourceStream = sourceStream;
			this.needList = needList;
			syncingBoundary = "syncing";
		}


		public async Task<SynchronizationReport> PushChangesAsync(CancellationToken token)
		{
			token.Register(() => { });//request.Abort() TODO: check this

			token.ThrowIfCancellationRequested();

			if (sourceStream.CanRead == false)
				throw new Exception("Stream does not support reading");

			request = destination.JsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, destination.FileSystemUrl + "/synchronization/MultipartProceed",
					                                                "POST", destination.Credentials, destination.Convention));

            // REVIEW: (Oren) There is a mismatch of expectations in the AddHeaders. ETag must always have to be surrounded by quotes. 
            //         If AddHeader/s ever put an etag it should check for that.
            //         I was hesitant to do the change though, because I do not understand the complete scope of such a change.
            request.AddHeaders(sourceMetadata);           
			request.AddHeader("Content-Type", "multipart/form-data; boundary=" + syncingBoundary);

			request.AddHeader(SyncingMultipartConstants.FileName, fileName);
			request.AddHeader(SyncingMultipartConstants.SourceServerInfo, serverInfo.AsJson());

			try
			{
				await request.WriteAsync(PrepareMultipartContent(token));

				var response = await request.ReadResponseJsonAsync();
				return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
			}
			catch (Exception exception)
			{
				if (token.IsCancellationRequested)
				{
					throw new OperationCanceledException(token);
				}

				var webException = exception as ErrorResponseException;

				if (webException != null)
				{
					webException.BetterWebExceptionError();
				}

				throw;
			}
		}

		internal MultipartContent PrepareMultipartContent(CancellationToken token)
		{
			var content = new CompressedMultiPartContent("form-data", syncingBoundary);

			foreach (var item in needList)
			{
				token.ThrowIfCancellationRequested();

				var @from = Convert.ToInt64(item.FileOffset);
				var length = Convert.ToInt64(item.BlockLength);
				var to = from + length - 1;

				switch (item.BlockType)
				{
					case RdcNeedType.Source:
						content.Add(new SourceFilePart(new NarrowedStream(sourceStream, from, to)));
						break;
					case RdcNeedType.Seed:
						content.Add(new SeedFilePart(@from, to));
						break;
					default:
						throw new NotSupportedException();
				}
			}

			return content;
		}

		public class CompressedMultiPartContent : MultipartContent
		{
			public CompressedMultiPartContent(string subtype, string boundary) : base(subtype, boundary)
			{
				Headers.ContentEncoding.Add("gzip");
				Headers.ContentLength = null;
			}

			protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				using (stream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
					await base.SerializeToStreamAsync(stream, context);
			}
		}


		public ProfilingInformation ProfilingInformation { get; private set; }
	}
}