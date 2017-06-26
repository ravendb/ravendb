using System.Collections.Generic;

namespace Raven.Client.Server.ETL
{
    public class RavenConnectionString : ConnectionString
    {
        private string _url;

        public string Database { get; set; }

        public string ApiKey { get; set; }

        public string Url
        {
            get => _url;
            set => _url = value?.TrimEnd("/");        }

        public override ConnectionStringType Type => ConnectionStringType.Raven;

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (string.IsNullOrEmpty(Database))
                errors.Add($"{nameof(Database)} cannot be empty");

            if (string.IsNullOrEmpty(Url))
                errors.Add($"{nameof(Url)} cannot be empty");
        }
    }
}