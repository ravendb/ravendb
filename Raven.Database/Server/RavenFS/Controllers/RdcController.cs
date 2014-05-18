using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class RdcController : RavenFsApiController
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/rdc/Signatures/{*id}")]
		public HttpResponseMessage Signatures(string id)
		{
			var filename = Uri.UnescapeDataString(id);

			Log.Debug("Got signatures of a file '{0}' request", filename);

			using (var signatureRepository = new StorageSignatureRepository(Storage, filename))
			{
				var localRdcManager = new LocalRdcManager(signatureRepository, Storage, SigGenerator);
				var resultContent = localRdcManager.GetSignatureContentForReading(filename);
				return StreamResult(filename, resultContent);
			}
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/rdc/Stats")]
		public HttpResponseMessage Stats()
		{
			using (var rdcVersionChecker = new RdcVersionChecker())
			{
				var rdcVersion = rdcVersionChecker.GetRdcVersion();

                var stats = new RdcStats
                {
                    CurrentVersion = rdcVersion.CurrentVersion,
                    MinimumCompatibleAppVersion = rdcVersion.MinimumCompatibleAppVersion
                };

                return this.GetMessageWithObject(stats, HttpStatusCode.OK);
			}
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/rdc/Manifest/{*id}")]
		public async Task<HttpResponseMessage> Manifest(string id)
		{
			var filename = Uri.UnescapeDataString(id);
			FileAndPages fileAndPages = null;
			try
			{
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(filename, 0, 0));
			}
			catch (FileNotFoundException)
			{
				Log.Debug("Signature manifest for a file '{0}' was not found", filename);
				return Request.CreateResponse(HttpStatusCode.NotFound);
			}

			long? fileLength = fileAndPages.TotalSize;

			using (var signatureRepository = new StorageSignatureRepository(Storage, filename))
			{
				var rdcManager = new LocalRdcManager(signatureRepository, Storage, SigGenerator);
				var signatureManifest = await rdcManager.GetSignatureManifestAsync(
                                                                new DataInfo
					                                            {
						                                            Name = filename,
						                                            CreatedAt = Convert.ToDateTime(fileAndPages.Metadata.Value<string>("Last-Modified"))
								                                                       .ToUniversalTime()
					                                            });
				signatureManifest.FileLength = fileLength ?? 0;

				Log.Debug("Signature manifest for a file '{0}' was downloaded. Signatures count was {1}", filename, signatureManifest.Signatures.Count);

                return this.GetMessageWithObject(signatureManifest, HttpStatusCode.OK);
			}
		}
	}
}