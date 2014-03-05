using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class MetadataUpdateWorkItem : SynchronizationWorkItem
	{
		private readonly NameValueCollection destinationMetadata;
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public MetadataUpdateWorkItem(string fileName, string sourceServerUrl, NameValueCollection destinationMetadata,
									  TransactionalStorage storage)
			: base(fileName, sourceServerUrl, storage)
		{
			this.destinationMetadata = destinationMetadata;
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.MetadataUpdate; }
		}

        public override async Task<SynchronizationReport> PerformAsync(SynchronizationDestination destination)
		{
			AssertLocalFileExistsAndIsNotConflicted(FileMetadata);

			var conflict = CheckConflictWithDestination(FileMetadata, destinationMetadata, ServerInfo.Url);

			if (conflict != null)
				return await ApplyConflictOnDestinationAsync(conflict, destination, ServerInfo.Url, log);

			var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
						destination.FileSystemUrl + "/synchronization/updatemetadata?fileName=" + Uri.EscapeDataString(FileName),
						"POST", new OperationCredentials("", new CredentialCache()), Convention));

			request.AddHeaders(FileMetadata);
			

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
			if (obj.GetType() != typeof(MetadataUpdateWorkItem)) return false;
			return Equals((MetadataUpdateWorkItem)obj);
		}

		public bool Equals(MetadataUpdateWorkItem other)
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
			return string.Format("Metadata synchronization of a file '{0}'", FileName);
		}
	}
}