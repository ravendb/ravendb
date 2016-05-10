using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Storage;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper
{
    public class StorageSignatureRepository : ISignatureRepository
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private readonly string _fileName;
        private readonly ITransactionalStorage _storage;
        private readonly string _tempDirectory;
        private IDictionary<string, FileStream> _createdFiles;

        public StorageSignatureRepository(ITransactionalStorage storage, string fileName)
        {
            _tempDirectory = TempDirectoryTools.Create();
            _storage = storage;
            _fileName = fileName;
            _createdFiles = new Dictionary<string, FileStream>();
        }

        public Stream GetContentForReading(string sigName)
        {
            SignatureReadOnlyStream signatureStream = null;
            _storage.Batch(
                accessor =>
                {
                    var signatureLevel = GetSignatureLevel(sigName, accessor);
                    if (signatureLevel != null)
                    {
                        signatureStream = new SignatureReadOnlyStream(_storage, signatureLevel.Id, signatureLevel.Level);
                    }
                    else
                    {
                        throw new FileNotFoundException(sigName + " not found in the repo");
                    }
                });
            signatureStream.Position = 0;
            return signatureStream;
        }

        public Stream CreateContent(string sigName)
        {
            var sigFileName = NameToPath(sigName);

            var signatureDirectory = Path.GetDirectoryName(sigFileName);

            IOExtensions.CreateDirectoryIfNotExists(signatureDirectory);

            var result = File.Create(sigFileName, 64 * 1024);
            log.Info("File {0} created", sigFileName);
            _createdFiles.Add(sigFileName, result);
            return result;
        }


        public void Flush(IEnumerable<SignatureInfo> signatureInfos)
        {
            if (_createdFiles.Count == 0)
                throw new ArgumentException("Must have at least one signature info", "signatureInfos");

            CloseCreatedStreams();

            _storage.Batch(
                accessor =>
                {
                    accessor.ClearSignatures(_fileName);
                    foreach (var item in _createdFiles)
                    {
                        var item1 = item;
                        var level = SignatureInfo.Parse(item.Key).Level;
                        accessor.AddSignature(_fileName, level,
                                              stream =>
                                              {
                                                  using (var cachedSigContent = File.OpenRead(item1.Key))
                                                  {
                                                      cachedSigContent.CopyTo(stream);
                                                  }
                                              });
                    }
                });
            _createdFiles = new Dictionary<string, FileStream>();
        }

        public IEnumerable<SignatureInfo> GetByFileName()
        {
            IList<SignatureInfo> result = null;
            _storage.Batch(
                accessor =>
                {
                    result = (from item in accessor.GetSignatures(_fileName)
                              orderby item.Level
                              select new SignatureInfo(item.Level, _fileName)
                              {
                                  Length = accessor.GetSignatureSize(item.Id, item.Level)
                              }).ToList();
                });
            if (!result.Any())
                throw new FileNotFoundException("Cannot find signatures for " + _fileName);

            return result;
        }

        public DateTime? GetLastUpdate()
        {
            DateTime? result = null;
            _storage.Batch(accessor =>
            {
                var signatures = accessor.GetSignatures(_fileName).ToList();

                if(signatures.Count == 0)
                    return;

                result = signatures.Min(x => x.CreatedAt);
            });

            return result;
        }

        public void Dispose()
        {
           CloseCreatedStreams();
           IOExtensions.DeleteDirectory(_tempDirectory);
        }

        public SignatureInfo GetByName(string sigName)
        {
            SignatureInfo result = null;
            _storage.Batch(
                accessor =>
                {
                    var signatureLevel = GetSignatureLevel(sigName, accessor);
                    if (signatureLevel == null)
                    {
                        throw new FileNotFoundException(sigName + " not found in the repo");
                    }
                    result = SignatureInfo.Parse(sigName);
                    result.Length = accessor.GetSignatureSize(signatureLevel.Id, signatureLevel.Level);
                });
            return result;
        }

        private static SignatureLevels GetSignatureLevel(string sigName, IStorageActionsAccessor accessor)
        {
            var fileNameAndLevel = ExtractFileNameAndLevel(sigName);
            var signatureLevels = accessor.GetSignatures(fileNameAndLevel.FileName);
            return signatureLevels.FirstOrDefault(item => item.Level == fileNameAndLevel.Level);
        }

        private static SignatureInfo ExtractFileNameAndLevel(string sigName)
        {
            return SignatureInfo.Parse(sigName);
        }

        private string NameToPath(string name)
        {
            if (Path.IsPathRooted(name))
            {
                string pathRoot = Path.GetPathRoot(name);
                name = name.Substring(pathRoot.Length);
            }
            return Path.GetFullPath(Path.Combine(_tempDirectory, name));
        }

        private void CloseCreatedStreams()
        {
            foreach (var item in _createdFiles)
            {
                item.Value.Dispose();
            }
        }
    }
}
