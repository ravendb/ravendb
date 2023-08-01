namespace Raven.Server.Smuggler.Migration
{
    public sealed class ExportDataV35
    {
        public string DownloadOptions { get; set; }

        public long ProgressTaskId { get; set; }
    }

    public sealed class ExportDataV3
    {
        public string SmugglerOptions { get; set; }
    }
}
