using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Documents.Operations.ETL
{
    public class Transformation
    {
        internal const string LoadTo = "loadTo";

        internal const string LoadAttachment = "loadAttachment";

        internal const string AddAttachment = "addAttachment";
        
        internal const string AttachmentMarker = "$attachment/";
        
        private static readonly Regex LoadToMethodRegex = new Regex($@"{LoadTo}(\w+)", RegexOptions.Compiled);
        private static readonly Regex LoadAttachmentMethodRegex = new Regex(LoadAttachment, RegexOptions.Compiled);
        private static readonly Regex AddAttachmentMethodRegex = new Regex(AddAttachment, RegexOptions.Compiled);

        private static readonly Regex Legacy_ReplicateToMethodRegex = new Regex(@"replicateTo(\w+)", RegexOptions.Compiled);

        private string[] _collections;

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public List<string> Collections { get; set; } = new List<string>();

        public bool ApplyToAllDocuments { get; set; }

        public string Script { get; set; }

        public bool IsHandlingAttachments { get; private set; }

        public virtual bool Validate(ref List<string> errors, EtlType type)
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
                {
                    string targetName;
                    switch (type)
                    {
                        case EtlType.Raven:
                            targetName = "Collection";
                            break;
                        case EtlType.Sql:
                            targetName = "Table";
                            break;
                        default:
                            throw new ArgumentException($"Unknown ETL type: {type}");

                    }

                    errors.Add($"No `loadTo<{targetName}Name>()` method call found in '{Name}' script");
                }

                if (Legacy_ReplicateToMethodRegex.Matches(Script).Count > 0)
                {
                    errors.Add($"Found `replicateTo<TableName>()` method in '{Name}' script which is not supported. " +
                               "If you are using the SQL replication script from RavenDB 3.x version then please use `loadTo<TableName>()` instead.");
                }

                IsHandlingAttachments = LoadAttachmentMethodRegex.Matches(Script).Count > 0 || AddAttachmentMethodRegex.Matches(Script).Count > 0;
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
                _collections[i] = match[i].Value.Substring(LoadTo.Length);
            }

            return _collections;
        }
    }
}
