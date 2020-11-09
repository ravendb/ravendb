using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From50001 : ISchemaUpdate
    {
        public int From => 50_001;
        public int To => 50_002;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
