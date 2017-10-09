namespace Raven.Server.Smuggler.Migration
{
    public class ExportDataV35
    {
        public string DownloadOptions { get; set; }

        public long ProgressTaskId { get; set; }
    }

    public class ExportDataV3
    {
        public string SmugglerOptions { get; set; }
    }
}
