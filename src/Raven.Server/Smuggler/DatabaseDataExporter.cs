using System.Threading.Tasks;
using Raven.Client.Smuggler;

namespace Raven.Server.Smuggler
{
    public class DatabaseDataExporter
    {
        public long? StartDocsEtag;
        public bool Incremental;

        public int? Limit;

        public async Task<ExportResult> Export(IDatabaseSmugglerDestination destination)
        {
        }
    }
}