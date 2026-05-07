using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Storage.Schema.Updates.Documents._62001
{
    namespace Raven.Server.Storage.Schema.Updates.Documents
    {
        public class From62000 : ISchemaUpdate
        {
            public int From => 62_000;
            public int To => 62_001;
            public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

            public bool Update(UpdateStep step)
            {
                return true;
            }
        }
    }
}
