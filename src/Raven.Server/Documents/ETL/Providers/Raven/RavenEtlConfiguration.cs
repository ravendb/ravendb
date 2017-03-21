using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlConfiguration : EtlProcessConfiguration
    {
        private static readonly Regex LoadToMethodRegex = new Regex($@"{EtlTransformer<object, object>.LoadTo}(\w+)", RegexOptions.Compiled);

        private string _url;
        private string[] _collections;

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

        public int? LoadRequestTimeoutInSec { get; set; } 

        public override bool Validate(out List<string> errors)
        {
            base.Validate(out errors);

            if (string.IsNullOrEmpty(Database))
                errors.Add($"{nameof(Database)} cannot be empty");

            if (string.IsNullOrEmpty(Url))
                errors.Add($"{nameof(Url)} cannot be empty");

            if (string.IsNullOrEmpty(Script) == false)
            {
                var collections = GetCollectionsFromScript();

                if (collections == null || collections.Length == 0)
                    errors.Add("No `loadTo[CollectionName]` method call found in the script");
            }

            return errors.Count == 0;
        }

        public string[] GetCollectionsFromScript()
        {
            if (_collections != null)
                return _collections;

            var match = LoadToMethodRegex.Matches(Script);

            if (match.Count == 0)
                return null;

            _collections = new string[match.Count];

            for (var i = 0; i < match.Count; i++)
            {
                _collections[i] = match[i].Value.Substring(EtlTransformer<object, object>.LoadTo.Length);
            }

            return _collections;
        }
    }
}