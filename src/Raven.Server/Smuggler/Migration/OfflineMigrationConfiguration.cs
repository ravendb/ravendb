using System;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client.ServerWide;

namespace Raven.Server.Smuggler.Migration
{
    public class OfflineMigrationConfiguration
    {
        public string DataDirectory { get; set; }
        public string OutputFilePath { get; set; }
        public string DataExporterFullPath { get; set; }
        public int? BatchSize { get; set; }
        public bool IsRavenFs { get; set; }
        public bool IsCompressed { get; set; }      
        public string JournalsPath { get; set; }
        public string EncryptionKey { get; set; }
        public string EncryptionAlgorithm { get; set; }
        public int? EncryptionKeyBitsSize { get; set; }
        public TimeSpan? Timeout { get; set; }
        public DatabaseRecord DatabaseRecord { get; set; }

        public (string Commandline, string TmpFile) GenerateExporterCommandLine()
        {
            string tmpFile = null;
            var sb = new StringBuilder();

            if (DataDirectory == null || DataExporterFullPath == null)
                throw new ArgumentNullException("The following arguments are mandatory: DataDirectory and DataExporterFullPath");
            if (OutputFilePath == null)
            {
                var rempDir = Path.GetTempPath();
                OutputFilePath = tmpFile = Path.Combine(rempDir, "export.ravendump");
            }
            sb.Append($"{EnsureStringsAreQouted(DataDirectory)} {EnsureStringsAreQouted(OutputFilePath)}");

            if (BatchSize.HasValue)
            {
                sb.Append($" -BatchSize {BatchSize.Value}");
            }

            if (IsRavenFs)
            {
                sb.Append(" --RavenFS");
            }

            if (IsCompressed)
            {
                sb.Append(" --Compression");
            }

            if (string.IsNullOrEmpty(JournalsPath) == false)
            {
                sb.Append($" -JournalsPath {EnsureStringsAreQouted(JournalsPath)}");
            }

            if (string.IsNullOrEmpty(EncryptionKey) == false && string.IsNullOrEmpty(EncryptionAlgorithm) == false && EncryptionKeyBitsSize.HasValue)
            {
                sb.Append($" -Encryption {EnsureStringsAreQouted(EncryptionKey)} {EnsureStringsAreQouted(EncryptionAlgorithm)} {EncryptionKeyBitsSize.Value}");
            }

            return (sb.ToString(), tmpFile);

            string EnsureStringsAreQouted(string path)
            {
                //The path is arleady qouted
                if (path.First() == '\"' && path.Last() == '\"')
                    return path;
                return $"\"{path}\"";

            }
        }
    }
}
