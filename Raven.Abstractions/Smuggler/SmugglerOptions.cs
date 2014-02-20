//-----------------------------------------------------------------------
// <copyright file="ExportSpec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Abstractions.Smuggler
{
    using System.Text.RegularExpressions;
    public class SmugglerOptions
    {
        private int batchSize;
	    private TimeSpan timeout;

	    public SmugglerOptions()
        {
            Filters = new List<FilterSetting>();
            BatchSize = 1024;
            OperateOnTypes = ItemType.Indexes | ItemType.Documents | ItemType.Attachments | ItemType.Transformers;
            Timeout = TimeSpan.FromSeconds(30);
            ShouldExcludeExpired = false;
	        StartDocsDeletionEtag = StartAttachmentsDeletionEtag = StartAttachmentsEtag = StartDocsEtag = Etag.Empty;
            Limit = int.MaxValue;
		    MaxStepsForTransformScript = 10*1000;
	        ExportDeletions = false;
        }

        public bool ExportDeletions { get; set; }

        /// <summary>
        /// Start exporting from the specified documents etag
        /// </summary>
        public Etag StartDocsEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified attachments etag
        /// </summary>
        public Etag StartAttachmentsEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified document deletion etag
        /// </summary>
        public Etag StartDocsDeletionEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified attachment deletion etag
        /// </summary>
        public Etag StartAttachmentsDeletionEtag { get; set; }

        /// <summary>
        /// The number of document or attachments or indexes or transformers to load in each call to the RavenDB database.
        /// </summary>
        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                if (value < 1)
                    throw new InvalidOperationException("Batch size cannot be zero or a negative number");
                batchSize = value;
            }
        }

        /// <summary>
        /// Specify the types to operate on. You can specify more than one type by combining items with the OR parameter.
        /// Default is all items.
        /// Usage example: OperateOnTypes = ItemType.Indexes | ItemType.Transformers | ItemType.Documents | ItemType.Attachments.
        /// </summary>
        public ItemType OperateOnTypes { get; set; }

        public int Limit { get; set; }

        /// <summary>
        /// Filters to use to filter the documents that we will export/import.
        /// </summary>
        public List<FilterSetting> Filters { get; set; }

		public virtual bool MatchFilters(RavenJToken item)
		{
			foreach (var filter in Filters)
			{
			    bool anyRecords = false;
				bool matchedFilter = false;
				foreach (var tuple in item.SelectTokenWithRavenSyntaxReturningFlatStructure(filter.Path))
				{
					if (tuple == null || tuple.Item1 == null)
						continue;

				    anyRecords = true;

					var val = tuple.Item1.Type == JTokenType.String
								? tuple.Item1.Value<string>()
								: tuple.Item1.ToString(Formatting.None);
					matchedFilter |= filter.Values.Any(value => String.Equals(val, value, StringComparison.OrdinalIgnoreCase)) ==
									 filter.ShouldMatch;
				}

                if (filter.ShouldMatch == false && anyRecords == false) // RDBQA-7
                    return true;

				if (matchedFilter == false)
					return false;
			}
			return true;
		}

        /// <summary>
        /// Should we exclude any documents which have already expired by checking the expiration meta property created by the expiration bundle
        /// </summary>
        public bool ShouldExcludeExpired { get; set; }

        public virtual bool ExcludeExpired(RavenJToken item, DateTime now)
        {
            var metadata = item.Value<RavenJObject>("@metadata");

            const string RavenExpirationDate = "Raven-Expiration-Date";

            // check for expired documents and exclude them if expired
            if (metadata == null)
            {
                return false;
            }
            var property = metadata[RavenExpirationDate];
            if (property == null)
                return false;

            DateTime dateTime;
            try
            {
                dateTime = property.Value<DateTime>();
            }
            catch (FormatException)
            {
                return false;
            }

            return dateTime < now;
		}

	    /// <summary>
	    /// The timeout for requests
	    /// </summary>
	    public TimeSpan Timeout
	    {
		    get
		    {
				return timeout;
		    }
		    set
		    {
			    if (value < TimeSpan.FromSeconds(5))
			    {
				    throw new InvalidOperationException("Timout value cannot be less then 5 seconds.");
			    }
				timeout = value;
		    }
	    }

        public bool Incremental { get; set; }

        public string TransformScript { get; set; }

        /// <summary>
        /// Maximum number of steps that transform script can have
        /// </summary>
        public int MaxStepsForTransformScript { get; set; }
    }

    public class SmugglerBetweenOptions
    {
        public RavenConnectionStringOptions From { get; set; }

        public RavenConnectionStringOptions To { get; set; }

		/// <summary>
		/// You can give a key to the incremental last etag, in order to make incremental imports from a few export sources.
		/// </summary>
		public string IncrementalKey { get; set; }
    }

    public class SmugglerExportOptions
    {
        public RavenConnectionStringOptions From { get; set; }

        /// <summary>
        /// The path to write the export.
        /// </summary>
        public string ToFile { get; set; }

        /// <summary>
        /// The stream to write the export.
        /// </summary>
        public Stream ToStream { get; set; }
    }

    public class SmugglerImportOptions
    {
        public RavenConnectionStringOptions To { get; set; }

        /// <summary>
        /// The path to read from of the import data.
        /// </summary>
        public string FromFile { get; set; }

        /// <summary>
        /// The stream to read from of the import data.
        /// </summary>
        public Stream FromStream { get; set; }
    }

	[Flags]
	public enum ItemType
	{
		Documents = 0x1,
		Indexes = 0x2,
		Attachments = 0x4,
		Transformers = 0x8,

        RemoveAnalyzers = 0x8000,
	}

	public class FilterSetting
	{
		public string Path { get; set; }
		public List<string> Values { get; set; }
		public bool ShouldMatch { get; set; }

		public FilterSetting()
		{
			Values = new List<string>();
		}

        private static readonly Regex Regex = new Regex(@"('[^']+'|[^,]+)");

	    public static List<string> ParseValues(string value)
	    {
            var results = new List<string>();

            if (string.IsNullOrEmpty(value))
                return results;

	        var matches = Regex.Matches(value);
	        for (var i = 0; i < matches.Count; i++)
	        {
	            var match = matches[i].Value;
	            
	            if (match.StartsWith("'") && match.EndsWith("'"))
                    match = match.Substring(1, match.Length - 2);

                results.Add(match);
	        }

	        return results;
	    }
	}
}
