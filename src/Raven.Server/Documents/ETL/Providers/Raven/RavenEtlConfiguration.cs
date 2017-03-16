namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlConfiguration : EtlProcessConfiguration
    {
        private string _url;

        public string Url
        {
            get { return _url; }
            set
            {
                _url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
            }
        }

        public string Database { get; set; }

        public string ApiKey { get; set; }
    }
}