using System;
using Microsoft.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Raven.Migrator.MongoDB;

namespace Raven.Migrator
{
    internal static class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private static CommandLineApplication _app;

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            _app = new CommandLineApplication
            {
                Name = "Raven.Migrator",
                Description = "Migration tool from other databases"
            };

            _app.HelpOption(HelpOptionString);

            ConfigureMigrationFromMongoDBCommand();

            _app.OnExecute(() =>
            {
                _app.ShowHelp();
                return 1;
            });

            try
            {
                return _app.Execute(args);
            }
            catch (CommandParsingException parsingException)
            {
                return ExitWithError(parsingException.Message, _app);
            }
        }

        private static void ConfigureMigrationFromMongoDBCommand()
        {
            _app.Command("mongodb", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Connect to MongoDB, pass configuration as JSON" + Environment.NewLine +
                                                         "   Command - available commands: databases, collections, export" + Environment.NewLine +
                                                         "   ConnectionString - MongoDB connection string" + Environment.NewLine +
                                                         "   DatbaseName - the database name, applicaple only for the collections and export commands" + Environment.NewLine +
                                                         "   MigrateGridFS - (Optional) migrate GridFS that is associated with the provided database" + Environment.NewLine +
                                                         "   CollectionsToMigrate - (Optional) a dictionary of collections to rename during the export";
                cmd.HelpOption(HelpOptionString);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var mongodbConfigurationString = Console.ReadLine();
                        var mongodbConfiguration = JsonConvert.DeserializeObject<MongoDBConfiguration>(mongodbConfigurationString);

                        if (string.IsNullOrWhiteSpace(mongodbConfiguration.Command))
                            return ExitWithError("Command cannot be null or empty", cmd);

                        var migrator = new MongoDBMigrator(mongodbConfiguration);
                        switch (mongodbConfiguration.Command)
                        {
                            case "databases":
                                migrator.GetDatabases().GetAwaiter().GetResult();
                                break;
                            case "collections":
                                migrator.GetCollectionsInfo().GetAwaiter().GetResult();
                                break;
                            case "export":
                                migrator.MigrateSingleDatabse().GetAwaiter().GetResult();
                                break;
                            default:
                                return ExitWithError($"Command '{mongodbConfiguration.Command}' doesn't exist" + Environment.NewLine +
                                                     "available commands: databases, collections, export", cmd);
                        }
                    }
                    catch (Exception e)
                    {
                        return ExitWithError($"Failed to run MongoDB command: {e}", cmd);
                    }

                    return 0;
                });
            });
        }

        private static int ExitWithError(string errMsg, CommandLineApplication cmd)
        {
            cmd.Error.WriteLine(errMsg);
            cmd.ShowHelp();
            return 1;
        }
    }
}
