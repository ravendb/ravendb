namespace Raven.Server.Smuggler
{
    public class ImportResult
    {
        public long DocumentsCount { get; set; }
        public long IndexesCount { get; set; }
        public long TransformersCount { get; set; }
        public long IdentitiesCount { get; set; }
    }
}