namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
        public ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
        ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
    }
}
