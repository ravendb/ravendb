using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class ContentUpdateWorkItem : SynchronizationWorkItem
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly SigGenerator sigGenerator;
		private DataInfo fileDataInfo;
		private SynchronizationMultipartRequest multipartRequest;

		public ContentUpdateWorkItem(string file, string sourceServerUrl, ITransactionalStorage storage, SigGenerator sigGenerator) : base(file, sourceServerUrl, storage)
		{
			this.sigGenerator = sigGenerator;
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.ContentUpdate; }
		}

		private DataInfo FileDataInfo
		{
			get { return fileDataInfo ?? (fileDataInfo = GetLocalFileDataInfo(FileName)); }
		}

		public override void Cancel()
		{
			Cts.Cancel();
		}

        public override async Task<SynchronizationReport> PerformAsync(RavenFileSystemClient.SynchronizationClient destination)
		{
			AssertLocalFileExistsAndIsNotConflicted(FileMetadata);

			var destinationMetadata = await destination.GetMetadataForAsync(FileName);

			if (destinationMetadata == null)
			{
				// if file doesn't exist on destination server - upload it there
                return await UploadToAsync(destination);
			}

			var destinationServerRdcStats = await destination.GetRdcStatsAsync();
			if (!IsRemoteRdcCompatible(destinationServerRdcStats))
				throw new SynchronizationException("Incompatible RDC version detected on destination server");

			var conflict = CheckConflictWithDestination(FileMetadata, destinationMetadata, ServerInfo.FileSystemUrl);

			if (conflict != null)
				return await ApplyConflictOnDestinationAsync(conflict, destination, ServerInfo.FileSystemUrl, log);

			using (var localSignatureRepository = new StorageSignatureRepository(Storage, FileName))
			using (var remoteSignatureCache = new VolatileSignatureRepository(FileName))
			{
				var localRdcManager = new LocalRdcManager(localSignatureRepository, Storage, sigGenerator);
				var destinationRdcManager = new RemoteRdcManager(destination, localSignatureRepository,
																 remoteSignatureCache);

				log.Debug("Starting to retrieve signatures of a local file '{0}'.", FileName);

				Cts.Token.ThrowIfCancellationRequested();

				// first we need to create a local file signatures before we synchronize with remote ones
				var localSignatureManifest = await localRdcManager.GetSignatureManifestAsync(FileDataInfo);

				log.Debug("Number of a local file '{0}' signatures was {1}.", FileName, localSignatureManifest.Signatures.Count);

				if (localSignatureManifest.Signatures.Count > 0)
				{
					var destinationSignatureManifest = await destinationRdcManager.SynchronizeSignaturesAsync(FileDataInfo, Cts.Token);

					if (destinationSignatureManifest.Signatures.Count > 0)
					{
						return
							await
                            SynchronizeTo(destination, localSignatureRepository, remoteSignatureCache, localSignatureManifest,
										  destinationSignatureManifest);
					}
				}

                return await UploadToAsync(destination);
			}
		}

		private bool IsRemoteRdcCompatible(RdcStats destinationServerRdcStats)
		{
			using (var versionChecker = new RdcVersionChecker())
			{
				var localRdcVersion = versionChecker.GetRdcVersion();
				return destinationServerRdcStats.CurrentVersion >= localRdcVersion.MinimumCompatibleAppVersion;
			}
		}

        private async Task<SynchronizationReport> SynchronizeTo(RavenFileSystemClient.SynchronizationClient destination,
																ISignatureRepository localSignatureRepository,
																ISignatureRepository remoteSignatureRepository,
																SignatureManifest sourceSignatureManifest,
																SignatureManifest destinationSignatureManifest)
		{
			var seedSignatureInfo = SignatureInfo.Parse(destinationSignatureManifest.Signatures.Last().Name);
			var sourceSignatureInfo = SignatureInfo.Parse(sourceSignatureManifest.Signatures.Last().Name);

			using (var localFile = StorageStream.Reading(Storage, FileName))
			{
				IList<RdcNeed> needList;
				using (var needListGenerator = new NeedListGenerator(remoteSignatureRepository, localSignatureRepository))
				{
					needList = needListGenerator.CreateNeedsList(seedSignatureInfo, sourceSignatureInfo, Cts.Token);
				}

				return await PushByUsingMultipartRequest(destination, localFile, needList);
			}
		}

        public async Task<SynchronizationReport> UploadToAsync(RavenFileSystemClient.SynchronizationClient destination)
		{
			using (var sourceFileStream = StorageStream.Reading(Storage, FileName))
			{
				var fileSize = sourceFileStream.Length;

				var onlySourceNeed = new List<RdcNeed>
					                     {
						                     new RdcNeed
							                     {
								                     BlockType = RdcNeedType.Source,
								                     BlockLength = (ulong) fileSize,
								                     FileOffset = 0
							                     }
					                     };

				return await PushByUsingMultipartRequest(destination, sourceFileStream, onlySourceNeed);
			}
		}

        private Task<SynchronizationReport> PushByUsingMultipartRequest(RavenFileSystemClient.SynchronizationClient destination, Stream sourceFileStream,
																		IList<RdcNeed> needList)
		{
			Cts.Token.ThrowIfCancellationRequested();

			multipartRequest = new SynchronizationMultipartRequest(destination, ServerInfo, FileName, FileMetadata, sourceFileStream, needList);

			var bytesToTransferCount = needList.Where(x => x.BlockType == RdcNeedType.Source).Sum(x => (double)x.BlockLength);

			log.Debug(
				"Synchronizing a file '{0}' (ETag {1}) to {2} by using multipart request. Need list length is {3}. Number of bytes that needs to be transfered is {4}",
				FileName, FileETag, destination, needList.Count, bytesToTransferCount);

			return multipartRequest.PushChangesAsync(Cts.Token);
		}

		private DataInfo GetLocalFileDataInfo(string fileName)
		{
			FileAndPages fileAndPages = null;

			try
			{
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
			}
			catch (FileNotFoundException)
			{
				return null;
			}

			return new DataInfo
			{
				CreatedAt = Convert.ToDateTime(fileAndPages.Metadata.Value<string>("Last-Modified")).ToUniversalTime(),
				Length = fileAndPages.TotalSize ?? 0,
				Name = fileAndPages.Name
			};
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(ContentUpdateWorkItem)) return false;
			return Equals((ContentUpdateWorkItem)obj);
		}

		public bool Equals(ContentUpdateWorkItem other)
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
			return string.Format("Synchronization of a file content '{0}'", FileName);
		}
	}
}
