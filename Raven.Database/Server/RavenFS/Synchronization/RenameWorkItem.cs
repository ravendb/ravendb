using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class RenameWorkItem : SynchronizationWorkItem
	{
		private readonly string rename;

		public RenameWorkItem(string name, string rename, string sourceServerUrl, TransactionalStorage storage)
			: base(name, sourceServerUrl, storage)
		{
			this.rename = rename;
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.Rename; }
		}

        public override async Task<SynchronizationReport> PerformAsync(SynchronizationDestination destination)
		{
			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
					destination.FileSystemUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(FileName) + "&rename=" +
								  Uri.EscapeDataString(rename),
					"PATCH", new OperationCredentials("", new CredentialCache()), Convention));

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
			if (obj.GetType() != typeof(RenameWorkItem)) return false;
			return Equals((RenameWorkItem)obj);
		}

		public bool Equals(RenameWorkItem other)
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
			return string.Format("Synchronization of a renaming of a file '{0}' to '{1}'", FileName, rename);
		}
	}
}