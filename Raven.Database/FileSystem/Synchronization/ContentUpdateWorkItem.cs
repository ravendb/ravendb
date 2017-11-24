using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Multipart;
using Raven.Database.FileSystem.Synchronization.Rdc;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Synchronization
{
    public class ContentUpdateWorkItem : SynchronizationWorkItem
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly SigGenerator sigGenerator;

        private readonly InMemoryRavenConfiguration configuration;

        private DataInfo fileDataInfo;
        private SynchronizationMultipartRequest multipartRequest;

        public ContentUpdateWorkItem(string file, string sourceServerUrl, ITransactionalStorage storage, SigGenerator sigGenerator, InMemoryRavenConfiguration configuration) : base(file, sourceServerUrl, storage)
        {
            this.sigGenerator = sigGenerator;
            this.configuration = configuration;
        }

        public override SynchronizationType SynchronizationType
        {
            get { return configuration.FileSystem.DisableRDC == false ? SynchronizationType.ContentUpdate : SynchronizationType.ContentUpdateNoRDC; }
        }

        private DataInfo FileDataInfo
        {
            get { return fileDataInfo ?? (fileDataInfo = GetLocalFileDataInfo(FileName)); }
        }

        public override void Cancel()
        {
            Cts.Cancel();
        }

        public override async Task<SynchronizationReport> PerformAsync(ISynchronizationServerClient synchronizationServerClient)
        {
            AssertLocalFileExistsAndIsNotConflicted(FileMetadata);

            if (configuration.FileSystem.DisableRDC)
                return await UploadToAsync(synchronizationServerClient).ConfigureAwait(false);

            var destinationMetadata = await synchronizationServerClient.GetMetadataForAsync(FileName).ConfigureAwait(false);
            if (destinationMetadata == null)
            {
                // if file doesn't exist on destination server - upload it there
                return await UploadToAsync(synchronizationServerClient).ConfigureAwait(false);
            }

            var destinationServerRdcStats = await synchronizationServerClient.GetRdcStatsAsync().ConfigureAwait(false);
            if (!IsRemoteRdcCompatible(destinationServerRdcStats))
                throw new SynchronizationException("Incompatible RDC version detected on destination server");

            var conflict = CheckConflictWithDestination(FileMetadata, destinationMetadata, FileSystemInfo.Url);
            if (conflict != null)
            {
                var report = await HandleConflict(synchronizationServerClient, conflict, log).ConfigureAwait(false);

                if (report != null)
                    return report;
            }

            using (var localSignatureRepository = new StorageSignatureRepository(Storage, FileName, configuration))
            using (var remoteSignatureCache = new VolatileSignatureRepository(FileName, configuration))
            {
                var localRdcManager = new LocalRdcManager(localSignatureRepository, Storage, sigGenerator);
                var destinationRdcManager = new RemoteRdcManager(synchronizationServerClient, localSignatureRepository, remoteSignatureCache);
                if (log.IsDebugEnabled)
                    log.Debug("Starting to retrieve signatures of a local file '{0}'.", FileName);

                Cts.Token.ThrowIfCancellationRequested();

                // first we need to create a local file signatures before we synchronize with remote ones
                var localSignatureManifest = await localRdcManager.GetSignatureManifestAsync(FileDataInfo).ConfigureAwait(false);
                if (log.IsDebugEnabled)
                    log.Debug("Number of a local file '{0}' signatures was {1}.", FileName, localSignatureManifest.Signatures.Count);

                if (localSignatureManifest.Signatures.Any())
                {
                    var destinationSignatureManifest = await destinationRdcManager.SynchronizeSignaturesAsync(FileDataInfo, Cts.Token).ConfigureAwait(false);
                    if (destinationSignatureManifest.Signatures.Any())
                    {
                        return await SynchronizeTo(synchronizationServerClient, localSignatureRepository, remoteSignatureCache, localSignatureManifest, destinationSignatureManifest).ConfigureAwait(false);
                    }
                }

                return await UploadToAsync(synchronizationServerClient).ConfigureAwait(false);
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

        private async Task<SynchronizationReport> SynchronizeTo(ISynchronizationServerClient synchronizationServerClient,
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

                return await PushByUsingMultipartRequest(synchronizationServerClient, localFile, needList).ConfigureAwait(false);
            }
        }

        public async Task<SynchronizationReport> UploadToAsync(ISynchronizationServerClient synchronizationServerClient)
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

                return await PushByUsingMultipartRequest(synchronizationServerClient, sourceFileStream, onlySourceNeed).ConfigureAwait(false);
            }
        }

        private Task<SynchronizationReport> PushByUsingMultipartRequest(ISynchronizationServerClient synchronizationServerClient, Stream sourceFileStream,
                                                                        IList<RdcNeed> needList)
        {
            Cts.Token.ThrowIfCancellationRequested();

            multipartRequest = new SynchronizationMultipartRequest(synchronizationServerClient, FileSystemInfo, FileName, FileMetadata, sourceFileStream, needList, SynchronizationType);

            var bytesToTransferCount = needList.Where(x => x.BlockType == RdcNeedType.Source).Sum(x => (double)x.BlockLength);
            if (log.IsDebugEnabled)
                log.Debug(
                "Synchronizing a file '{0}' (ETag {1}) to {2} by using multipart request. Need list length is {3}. Number of bytes that needs to be transfered is {4}",
                FileName, FileETag, synchronizationServerClient, needList.Count, bytesToTransferCount);

            return multipartRequest.PushChangesAsync(Cts.Token);
        }

        private DataInfo GetLocalFileDataInfo(string fileName)
        {
            FileAndPagesInformation fileAndPages = null;

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
                LastModified = fileAndPages.Metadata.Value<DateTime>(Constants.LastModified).ToUniversalTime(),
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
