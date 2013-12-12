using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Synchronization.Multipart
{
	public class SynchronizationMultipartRequest
	{
		private readonly string destinationUrl;
		private readonly string fileName;
		private readonly IList<RdcNeed> needList;
		private readonly ServerInfo serverInfo;
		private readonly NameValueCollection sourceMetadata;
		private readonly Stream sourceStream;
		private readonly string syncingBoundary;
		private HttpWebRequest request;

		public SynchronizationMultipartRequest(string destinationUrl, ServerInfo serverInfo, string fileName,
											   NameValueCollection sourceMetadata, Stream sourceStream,
											   IList<RdcNeed> needList)
		{
			this.destinationUrl = destinationUrl;
			this.serverInfo = serverInfo;
			this.fileName = fileName;
			this.sourceMetadata = sourceMetadata;
			this.sourceStream = sourceStream;
			this.needList = needList;
			syncingBoundary = "syncing";
		}

		public async Task<SynchronizationReport> PushChangesAsync(CancellationToken token)
		{
			token.Register(() => request.Abort());

			token.ThrowIfCancellationRequested();

			if (sourceStream.CanRead == false)
			{
				throw new Exception("Stream does not support reading");
			}

			request = (HttpWebRequest)WebRequest.Create(destinationUrl + "/ravenfs/synchronization/MultipartProceed");
			request.Method = "POST";
			request.SendChunked = true;
			request.AllowWriteStreamBuffering = false;
			request.KeepAlive = true;

			request.AddHeaders(sourceMetadata);

			request.ContentType = "multipart/form-data; boundary=" + syncingBoundary;

			request.Headers[SyncingMultipartConstants.FileName] = fileName;
			request.Headers[SyncingMultipartConstants.SourceServerInfo] = serverInfo.AsJson();

			try
			{
				using (var requestStream = await request.GetRequestStreamAsync())
				{
					await PrepareMultipartContent(token).CopyToAsync(requestStream);

					using (var respose = await request.GetResponseAsync())
					{
						using (var responseStream = respose.GetResponseStream())
						{
							return
								new JsonSerializer().Deserialize<SynchronizationReport>(new JsonTextReader(new StreamReader(responseStream)));
						}
					}
				}
			}
			catch (Exception exception)
			{
				if (token.IsCancellationRequested)
				{
					throw new OperationCanceledException(token);
				}

				var webException = exception as WebException;

				if (webException != null)
				{
					webException.BetterWebExceptionError();
				}

				throw;
			}
		}

		internal MultipartContent PrepareMultipartContent(CancellationToken token)
		{
			var content = new MultipartContent("form-data", syncingBoundary);

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
	}
}