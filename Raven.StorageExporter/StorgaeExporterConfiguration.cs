using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.StorageExporter
{
    public class StorgaeExporterConfiguration
    {
        public string DatabaseDataDir { get; set; }
        public string OutputDumpPath { get; set; }
        public string TableName { get; set; }
        public int BatchSize { get; set; }
        public static int DefualtBatchSize = 1024;

        public Etag DocumentsStartEtag { get { return documentsStartEtag; } set { documentsStartEtag = value; } }
        private Etag documentsStartEtag = Etag.Empty;
        public void Export()
        {
            if (TableName != null)
            {
                using (var esentExportOperation = new EsentExportOperation(DatabaseDataDir))
                {
                    esentExportOperation.ExportTable(TableName, OutputDumpPath);
                }
            }
            else
            {
                int batchSize = BatchSize == 0 ? DefualtBatchSize : BatchSize;
                var storageExporter = new StorageExporter(DatabaseDataDir, OutputDumpPath, batchSize, DocumentsStartEtag);
                storageExporter.ExportDatabase();
            }
        }
    }
}
