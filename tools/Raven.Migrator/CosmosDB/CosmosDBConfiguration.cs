namespace Raven.Migrator.CosmosDB
{
    public class CosmosDBConfiguration : AbstractMigrationConfiguration
    {
        public string AzureEndpointUrl { get; set; }

        public string PrimaryKey { get; set; }
    }
}
