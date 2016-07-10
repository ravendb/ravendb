using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerOptions
    {
        public DatabaseSmugglerOptions()
        {
            Filters = new List<FilterSetting>();
            ConfigureDefaultFilters();
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Documents | DatabaseItemType.Transformers;
        }

        public long? StartDocsEtag { get; set; }

        public DatabaseItemType OperateOnTypes { get; set; }

        public bool IgnoreErrorsAndContinue { get; set; }

        public long? Limit { get; set; }

        public List<FilterSetting> Filters { get; set; }

        public string TransformScript { get; set; }

        public bool SkipConflicted { get; set; }

        public bool StripReplicationInformation { get; set; }

        public int MaxStepsForTransformScript { get; set; }

        public string Database { get; set; }

        public bool MatchFilters(RavenJObject document)
        {
            foreach (var filter in Filters)
            {
                bool anyRecords = false;
                bool matchedFilter = false;
                foreach (var tuple in document.SelectTokenWithRavenSyntaxReturningFlatStructure(filter.Path))
                {
                    if (tuple?.Item1 == null)
                        continue;

                    anyRecords = true;

                    var val = tuple.Item1.Type == JTokenType.String
                                  ? tuple.Item1.Value<string>()
                                  : tuple.Item1.ToString(Formatting.None);
                    matchedFilter |= filter.Values.Any(value => string.Equals(val, value, StringComparison.OrdinalIgnoreCase)) ==
                                     filter.ShouldMatch;
                }

                if (filter.ShouldMatch == false && anyRecords == false) // RDBQA-7
                    return true;

                if (matchedFilter == false)
                    return false;
            }
            return true;
        }

        private void ConfigureDefaultFilters()
        {
            // filter out encryption verification key document to enable import to encrypted db from encrypted db.
            Filters.Add(new FilterSetting
            {
                Path = "@metadata.@id",
                ShouldMatch = false,
                Values = { Constants.InResourceKeyVerificationDocumentName }
            });
        }
    }
}