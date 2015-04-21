using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.DataExporter
{
    public class DataExporter
    {
        public DataExporter(DataExporterConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Export()
        {
            if (configuration.TableName != null)
            {
                ExportTable(configuration.DatabaseDataDir, configuration.TableName, configuration.OutputDumpPath);
            }
            else
            {
                ExportDB(configuration.DatabaseDataDir, configuration.OutputDumpPath);
            }
        }

        private void ExportDB(string databaseDataDir, string outputDumpPath)
        {
            var exporter = new DatabaseExporter(databaseDataDir, outputDumpPath);
            exporter.Export();
        }

        private void ExportTable(string databaseDataDir, string tableName, string outputDumpPath)
        {
            using (var exporter = new TableExporter(configuration.DatabaseDataDir))
            {
                exporter.ExportTable(configuration.TableName, configuration.OutputDumpPath);
            }
        }

        private readonly DataExporterConfiguration configuration;
    }
}
