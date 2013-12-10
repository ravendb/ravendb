using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Imports.Newtonsoft.Json;

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

		public override async Task<SynchronizationReport> PerformAsync(string destination)
		{
			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));

			var request =
				(HttpWebRequest)
				WebRequest.Create(destination + "/synchronization/rename?filename=" + Uri.EscapeDataString(FileName) + "&rename=" +
								  Uri.EscapeDataString(rename));

			request.Method = "PATCH";
			request.ContentLength = 0;
			request.AddHeaders(fileAndPages.Metadata);

			request.Headers[SyncingMultipartConstants.SourceServerInfo] = ServerInfo.AsJson();

			try
			{
				using (var response = await request.GetResponseAsync())
				{
					using (var stream = response.GetResponseStream())
					{
						return new JsonSerializer().Deserialize<SynchronizationReport>(new JsonTextReader(new StreamReader(stream)));
					}
				}
			}
			catch (WebException exception)
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