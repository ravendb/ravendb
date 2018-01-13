namespace Raven.Server.Smuggler.Migration
{
    public class LastEtagsInfo
    {
        public const string EtagEmpty = "00000000-0000-0000-0000-000000000000";

        public LastEtagsInfo()
        {
            LastDocsEtag = EtagEmpty;
            LastAttachmentsEtag = EtagEmpty;
            LastDocDeleteEtag = EtagEmpty;
            LastAttachmentsDeleteEtag = EtagEmpty;
            LastRavenFsEtag = EtagEmpty;
        }

        public string ServerUrl { get; set; }

        public string DatabaseName { get; set; }

        public string LastDocsEtag { get; set; }

        public string LastDocDeleteEtag { get; set; }

        public string LastAttachmentsEtag { get; set; }

        public string LastAttachmentsDeleteEtag { get; set; }

        public string LastRavenFsEtag { get; set; }
    }
}
