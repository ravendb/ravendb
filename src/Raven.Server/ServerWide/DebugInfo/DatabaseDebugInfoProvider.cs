using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Org.BouncyCastle.X509;
using Raven.Client.Server;
using Raven.Server.Documents;

namespace Raven.Server.ServerWide.DebugInfo
{
    public class DatabaseDebugInfoProvider
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly string _databaseName;

        public DatabaseDebugInfoProvider(DocumentDatabase database)
        {            
            _databaseRecord = database.ServerStore.LoadDatabaseRecord(database.Name, out _);
            _databaseName = database.Name;
        }

        public IEnumerable<IDebugInfoDataSource> GetDebugPackage()
        {
            foreach (var node in _databaseRecord.Topology.AllNodes)
            {

            }

            return Enumerable.Empty<IDebugInfoDataSource>();
        }       
    }
}
