using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;

namespace Raven.Database.Plugins
{
    public class DeleteTemporaryIndexes : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            database.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp_"))
                .Apply(x => database.DeleteIndex(x));
        }
    }
}
