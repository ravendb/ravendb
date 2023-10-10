namespace Raven.Client.Documents.Smuggler
{
    public sealed class DatabaseSmugglerExportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerExportOptions
    {
        public ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
    }

    internal interface IDatabaseSmugglerExportOptions : IDatabaseSmugglerOptions
    {
        ExportCompressionAlgorithm? CompressionAlgorithm { get; set; }
    }
}
