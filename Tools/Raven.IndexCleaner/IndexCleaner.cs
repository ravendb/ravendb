using System;
using System.IO;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;

namespace Raven.IndexCleaner
{
    public class IndexCleaner
    {
        private readonly string databasePath;

        public IndexCleaner(string path)
        {
            databasePath = path;
            var ravenConfiguration = new RavenConfiguration();
            ravenConfiguration.DataDirectory = path;
            CreateTransactionalStorage(ravenConfiguration);
        }

        public void Clean()
        {
            using (storage)
            {
                storage.DropAllIndexingInformation();
            }

            var indexDir = Path.Combine(databasePath, "indexes");

            // We might run this tool twice and the directory was already deleted.
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
            }
        }

        public static void TryToCreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration, out ITransactionalStorage storage)
        {
            if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, "Data")) == false)
                throw new InvalidOperationException(string.Format("Cannot find the Data file in the '{0}' directory", ravenConfiguration.DataDirectory));

            storage = ravenConfiguration.CreateTransactionalStorage(() => { });

            storage?.Initialize(new SequentialUuidGenerator {EtagBase = 0},
                new OrderedPartCollection<AbstractDocumentCodec>());
        }

        private ITransactionalStorage storage;
    }
}