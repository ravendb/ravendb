using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Org.BouncyCastle.Security;

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
        public string DatabaseName { get; set; }
        public string EncryptionKey { get; set; }
        public string EncryptionAlgorithm { get; set; }
        public int? EncryptionKeyBitsSize { get; set; }
        public TimeSpan? Timeout { get; set; }

        public string GenerateExporterCommandLine()
        {
            var sb = new StringBuilder();

            if (DataDirectory == null || OutputFilePath == null || DataExporterFullPath == null)
                throw new ArgumentNullException("The following arguments are mandatory: DataDirectory, OutputFilePath and DataExporterFullPath");

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
                sb.Append($" -Encryption {EnsureStringsAreQouted(EncryptionKey)} {EncryptionAlgorithm} {EncryptionKeyBitsSize.Value}");
            }

            return sb.ToString();

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
