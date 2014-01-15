using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class DeleteWorkItem : SynchronizationWorkItem
	{
		public DeleteWorkItem(string fileName, string sourceServerUrl, TransactionalStorage storage)
			: base(fileName, sourceServerUrl, storage)
		{
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.Delete; }
		}

		public override async Task<SynchronizationReport> PerformAsync(string destination)
		{
			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));
			
			var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
						destination + "/ravenfs/synchronization?fileName=" + Uri.EscapeDataString(FileName),
						"DELETE", new OperationCredentials("", new CredentialCache()), Convention));


			
			request.AddHeaders(fileAndPages.Metadata);

			request.AddHeader(SyncingMultipartConstants.SourceServerInfo, ServerInfo.AsJson());

			try
			{
				var response = await request.ReadResponseJsonAsync();
				return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
			}
			catch (ErrorResponseException exception)
			{
				throw exception.BetterWebExceptionError();
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(DeleteWorkItem)) return false;
			return Equals((DeleteWorkItem)obj);
		}

		public bool Equals(DeleteWorkItem other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.FileName, FileName) && Equals(other.FileETag, FileETag);
		}

		public override int GetHashCode()
		{
			return (FileName != null ? GetType().Name.GetHashCode() ^ FileName.GetHashCode() ^ FileETag.GetHashCode() : 0);
		}

		public override string ToString()
		{
			return string.Format("Synchronization of a deleted file '{0}'", FileName);
		}
	}
}