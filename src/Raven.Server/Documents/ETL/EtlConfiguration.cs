namespace Raven.Server.Documents.ETL
{
    public class EtlConfiguration
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public string Collection { get; set; }

        public string Script { get; set; }
    }
}