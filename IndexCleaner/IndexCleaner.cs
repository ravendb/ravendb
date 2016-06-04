using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;

namespace Raven.IndexCleaner
{
    public class IndexCleaner
    {
        private string databasePath;

        public IndexCleaner(string path)
        {
            databasePath = path;
            var ravenConfiguration = new RavenConfiguration();
            ravenConfiguration.DataDirectory = path;
            CreateTransactionalStorage(ravenConfiguration);            
        }

        public void Clean()
        {
            storage.DropAllIndexingInformation();
            storage.Dispose();
            var indexDir = Path.Combine(databasePath, "indexes");
            //We might run this tool twice and the directory was already deleted.
            if (Directory.Exists(indexDir))
                Directory.Delete(indexDir, true);
        }

        private void CreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration)
        {
            try
            {
                TryToCreateTransactionalStorage(ravenConfiguration, out storage);
            }
            catch (UnauthorizedAccessException uae)
            {
                ConsoleUtils.PrintErrorAndFail($"Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{uae.Message}", uae.StackTrace);
            }
            catch (InvalidOperationException ioe)
            {
                ConsoleUtils.PrintErrorAndFail($"Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{ioe.Message}", ioe.StackTrace);
            }
            catch (Exception e)
            {
                ConsoleUtils.PrintErrorAndFail(e.Message, e.StackTrace);
                return;
            }
        }

        public static bool TryToCreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration, out ITransactionalStorage storage)
        {
            storage = null;
            if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
                storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.VoronTypeName, () => { }, () => { });
            else if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, "Data")))
                storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.EsentTypeName, () => { }, () => { });
            if (storage != null)
            {
                storage.Initialize(new SequentialUuidGenerator { EtagBase = 0 }, new OrderedPartCollection<AbstractDocumentCodec>());
                return true;
            }
            return false;
        }

        private ITransactionalStorage storage;
    }
}
