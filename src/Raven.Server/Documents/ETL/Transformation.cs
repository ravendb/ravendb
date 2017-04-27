using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.ETL
{
    public class Transformation
    {
        private static readonly Regex LoadToMethodRegex = new Regex($@"{EtlTransformer<ExtractedItem, object>.LoadTo}(\w+)", RegexOptions.Compiled);
        private static readonly Regex LoadAttachmentMethodRegex = new Regex(EtlTransformer<ExtractedItem, object>.LoadAttachment, RegexOptions.Compiled);

        private string[] _collections;

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public List<string> Collections { get; set; } = new List<string>();

        public bool ApplyToAllDocuments { get; set; }

        public string Script { get; set; }

        public bool HasLoadAttachment { get; private set; }

        public virtual bool Validate(ref List<string> errors)
        {
            if (errors == null)
                throw new ArgumentNullException(nameof(errors));

            if (string.IsNullOrWhiteSpace(Name))
                errors.Add("Script name cannot be empty");

            if (ApplyToAllDocuments)
            {
                if (Collections != null && Collections.Count > 0)
                    errors.Add($"{nameof(Collections)} cannot be specified when {nameof(ApplyToAllDocuments)} is set");
            }
            else
            {
                if (Collections == null || Collections.Count == 0)
                    errors.Add($"{nameof(Collections)} need be specified or {nameof(ApplyToAllDocuments)} has to be set");
            }

            if (string.IsNullOrEmpty(Script) == false)
            {
                var collections = GetCollectionsFromScript();

                if (collections == null || collections.Length == 0)
                    errors.Add("No `loadTo[CollectionName]` method call found in the script");

                HasLoadAttachment = LoadAttachmentMethodRegex.Matches(Script).Count > 0;
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
                _collections[i] = match[i].Value.Substring(EtlTransformer<ExtractedItem, object>.LoadTo.Length);
            }

            return _collections;
        }
    }
}