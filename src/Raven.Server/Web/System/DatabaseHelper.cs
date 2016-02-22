using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Utils;

namespace Raven.Server.Web.System
{
    public static class DatabaseHelper
    {
        public static bool CheckExistingDatabaseName(BlittableJsonReaderObject database, string id, string dbId, string etag, out string errorMessage)
        {
            var isExistingDatabase = database != null;
            if (isExistingDatabase && etag == null)
            {
                errorMessage = $"Database with the name '{id}' already exists";
                return false;
            }
            if (!isExistingDatabase && etag != null)
            {
                errorMessage = $"Database with the name '{id}' doesn't exist";
                return false;
            }

            errorMessage = null;
            return true;
        }

        public static void DeleteDatabaseFiles(RavenConfiguration configuration)
        {
            if (configuration.Core.RunInMemory)
                return;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);

            if (configuration.Indexing.IndexStoragePath != null)
                IOExtensions.DeleteDirectory(configuration.Indexing.IndexStoragePath);

            if (configuration.Storage.JournalsStoragePath != null)
                IOExtensions.DeleteDirectory(configuration.Storage.JournalsStoragePath);
        }
    }
}