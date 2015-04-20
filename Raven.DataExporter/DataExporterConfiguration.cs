using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.DataExporter
{
    public class DataExporterConfiguration
    {
        public bool IncludeAttachments { get; set; }
        public string DatabaseDataDir { get; set; }
        public string OutputDumpPath { get; set; }
        public string TableName { get; set; }
    }
}
