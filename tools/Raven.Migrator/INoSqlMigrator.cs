using System.Threading.Tasks;

namespace Raven.Migrator
{
    public interface INoSqlMigrator
    {
        Task GetDatabases();

        Task GetCollectionsInfo();

        Task MigrateDatabse();
    }
}
