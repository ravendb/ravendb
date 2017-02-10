namespace Raven.NewClient.Abstractions.Data
{
    public class BuildNumber
    {
        public string ProductVersion { get; set; }
        public int BuildVersion { get; set; }
        public string CommitHash { get; set; }
        public string FullVersion { get; set; }
    }
}
