namespace Raven.Server.Documents.ETL
{
    public class EtlProcessConfiguration
    {
        public string Id { get; set; } // TODO arek - to remove

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public string Collection { get; set; }

        public string Script { get; set; }
    }
}