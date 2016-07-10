using System.Threading.Tasks;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmuggler
    {
        public Task ExportAsync(DatabaseSmugglerOptions options, IDatabaseSmugglerDestination destination)
        {
            throw new System.NotImplementedException();
        }

        public Task ImportAsync(DatabaseSmugglerOptions options, IDatabaseSmugglerDestination destination)
        {
            throw new System.NotImplementedException();
        }
    }
}