using System;
using System.Security.Cryptography;
using Raven.Abstractions.Data;

namespace Raven.StorageExporter
{
    public class StorgaeExporterConfiguration
    {
        public string DatabaseDataDir { get; set; }
        public string OutputDumpPath { get; set; }
        public string JournalsPath { get; set; }
        public string LogFileSize { get; set; }
        public string MaxVerPages { get; set; }
        public string TableName { get; set; }
        public int BatchSize { get; set; }
        public bool IsRavendbFs { get; set; }

        public static int DefaultBatchSize = 1024;
        public bool HasCompression { get; set; }
        public EncryptionConfiguration Encryption { get; set; }

        public Etag DocumentsStartEtag { get { return documentsStartEtag; } set { documentsStartEtag = value; } }
        private Etag documentsStartEtag = Etag.Empty;

        public void Export()
        {
            if (TableName != null)
            {
                using (var esentExportOperation = new EsentExportOperation(DatabaseDataDir, HasCompression, Encryption))
                {
                    esentExportOperation.ExportTable(TableName, OutputDumpPath);
                }
            }
            else
            {
                int batchSize = BatchSize == 0 ? DefaultBatchSize : BatchSize;

                using (var storageExporter = new StorageExporter(DatabaseDataDir, OutputDumpPath, batchSize, DocumentsStartEtag, HasCompression, Encryption, JournalsPath, IsRavendbFs, LogFileSize, MaxVerPages))
                {
                    if (IsRavendbFs)
                    {
                        storageExporter.ExportFilesystemAsAttachments();
                    }
                    else
                    {
                        storageExporter.ExportDatabase();
                    }
                    
                }  
            }
        }
    }

    public class EncryptionConfiguration
    {
        public byte[] EncryptionKey { get; private set; }

        public Type SymmetricAlgorithmType { get; private set; }

        public bool EncryptIndexes { get; private set; }

        public int PreferedEncryptionKeyBitsSize { get; private set; }

        public bool TrySavingEncryptionKey(string encryptionKey)
        {
            try
            {
                EncryptionKey = Convert.FromBase64String(encryptionKey);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool SavePreferedEncryptionKeyBitsSize(string preferedEncryptionKeyBitsSize)
        {
            var result = 0;
            var actionResult = int.TryParse(preferedEncryptionKeyBitsSize, out result);
            PreferedEncryptionKeyBitsSize = result;
            return actionResult;
        }

        public bool TrySavingAlgorithmType(string algorithmType)
        {
            switch (algorithmType)
            {
                case "DESCryptoServiceProvider":
                case "DES":
                    SymmetricAlgorithmType = typeof (DESCryptoServiceProvider);
                    break;
                case "RC2CryptoServiceProvider":
                case "RC2":
                    SymmetricAlgorithmType = typeof (RC2CryptoServiceProvider);
                    break;
                case "RijndaelManaged":
                case "Rijndael":
                    SymmetricAlgorithmType = typeof (RijndaelManaged);
                    break;
                case "TripleDESCryptoServiceProvider":
                case "Triple DES":
                    SymmetricAlgorithmType = typeof (TripleDESCryptoServiceProvider);
                    break;
                default:
                    return false;
            }

            return true;
        }
    }
}
