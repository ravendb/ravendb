using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public class Transformation
    {
        internal const string LoadTo = "loadTo";

        internal const string LoadAttachment = "loadAttachment";
        
        internal const string AttachmentMarker = "$attachment/";
        
        private static readonly Regex LoadToMethodRegex = new Regex($@"{LoadTo}(\w+)", RegexOptions.Compiled);
        private static readonly Regex LoadAttachmentMethodRegex = new Regex(LoadAttachment, RegexOptions.Compiled);

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
                    errors.Add($"{nameof(Collections)} cannot be specified when {nameof(ApplyToAllDocuments)} is set. Script name: '{Name}'");
            }
            else
            {
                if (Collections == null || Collections.Count == 0)
                    errors.Add($"{nameof(Collections)} need be specified or {nameof(ApplyToAllDocuments)} has to be set. Script name: '{Name}'");
            }

            if (string.IsNullOrEmpty(Script) == false)
            {
                var collections = GetCollectionsFromScript();

                if (collections == null || collections.Length == 0)
                    errors.Add($"No `loadTo[CollectionName]` method call found in '{Name}' script");

                HasLoadAttachment = LoadAttachmentMethodRegex.Matches(Script).Count > 0;
            }

            return errors.Count == 0;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Script)] = Script,
                [nameof(Collections)] = new DynamicJsonArray(Collections),
                [nameof(ApplyToAllDocuments)] = ApplyToAllDocuments,
                [nameof(Disabled)] = Disabled
            };
        }

        public bool IsEqual(Transformation transformation)
        {
            if (transformation == null)
                return false;

            if (transformation.Collections.Count != Collections.Count)
                return false;

            var collections = new List<string>(Collections);

            foreach (var collection in transformation.Collections)
            {
                collections.Remove(collection);
            }

            return collections.Count == 0 &&
                   transformation.Name == Name &&
                   transformation.Script == Script &&
                   transformation.ApplyToAllDocuments == ApplyToAllDocuments &&
                   transformation.Disabled == Disabled;
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
                _collections[i] = match[i].Value.Substring(LoadTo.Length);
            }

            return _collections;
        }
    }
}
