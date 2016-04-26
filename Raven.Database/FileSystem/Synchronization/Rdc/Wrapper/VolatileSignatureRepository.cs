using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Infrastructure;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper
{
    public class VolatileSignatureRepository : ISignatureRepository
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private readonly IDictionary<string, FileStream> _createdFiles;
        private readonly string _fileName;
        private readonly string _tempDirectory;

        public VolatileSignatureRepository(string fileName, InMemoryRavenConfiguration configuration)
        {
            _tempDirectory = TempDirectoryTools.Create(configuration);
            _fileName = fileName;
            _createdFiles = new Dictionary<string, FileStream>();
        }

        public Stream GetContentForReading(string sigName)
        {
            Flush(null);
            return File.OpenRead(NameToPath(sigName));
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
            CloseCreatedStreams();
        }

        public IEnumerable<SignatureInfo> GetByFileName()
        {
            return from item in GetSigFileNamesByFileName()
                   select SignatureInfo.Parse(item);
        }

        public DateTime? GetLastUpdate()
        {
            var preResult = (from item in GetSigFileNamesByFileName()
                            let lastWriteTime = new FileInfo(item).LastWriteTime
                            orderby lastWriteTime descending
                            select lastWriteTime).ToList();
            if (preResult.Any())
                return preResult.First();

            return null;
        }

        public void Dispose()
        {
            CloseCreatedStreams();
            IOExtensions.DeleteDirectory(_tempDirectory);
        }

        private IEnumerable<string> GetSigFileNamesByFileName()
        {
            var fullPath = NameToPath(_fileName);
            var directory = Path.GetDirectoryName(fullPath);

            if (Directory.Exists(directory) == false)
                return Enumerable.Empty<string>();

            var fileName = Path.GetFileName(fullPath);

            return Directory.GetFiles(directory, fileName + "*.sig");
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

            _createdFiles.Clear();
        }
    }
}
