using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Database.Plugins.Builtins
{
    public class DeleteTemporaryIndexes : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            database.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp_"))
                .Apply(database.DeleteIndex);
        }
    }
}
