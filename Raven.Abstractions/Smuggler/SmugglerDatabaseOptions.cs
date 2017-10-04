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
using System.Text.RegularExpressions;
using Raven.Abstractions.Util;
using Raven.Database.Util;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerDatabaseOptions : SmugglerOptions<RavenConnectionStringOptions>
    {
        public new const int DefaultDocumentSizeInChunkLimitInBytes = 8 * 1024 * 1024;
        private int chunkSize;
       
        private long totalDocumentSizeInChunkLimitInBytes;

        public SmugglerDatabaseOptions()
        {
            DisableCompressionOnImport = false;
            Filters = new List<FilterSetting>();
            ConfigureDefaultFilters();
            ChunkSize = int.MaxValue;
            OperateOnTypes = ItemType.Indexes | ItemType.Documents | ItemType.Attachments | ItemType.Transformers;
            Timeout = TimeSpan.FromSeconds(30);
            ShouldExcludeExpired = false;			
            StartDocsDeletionEtag = StartAttachmentsDeletionEtag = StartAttachmentsEtag = StartDocsEtag = Etag.Empty;
            MaxStepsForTransformScript = 10*1000;
            ExportDeletions = false;
            TotalDocumentSizeInChunkLimitInBytes = DefaultDocumentSizeInChunkLimitInBytes;
            HeartbeatLatency = TimeSpan.FromSeconds(10);
        }

        private void ConfigureDefaultFilters()
        {
            // filter out encryption verification key document to enable import to encrypted db from encrypted db.
            Filters.Add(new FilterSetting
             {
                 Path = "@metadata.@id",
                 ShouldMatch = false,
                 Values = {Constants.InResourceKeyVerificationDocumentName}
             });
        }

        /// <summary>
        /// if true, smuggler will not halt its operation on errors, but instead will log them.
        /// </summary>
        public bool IgnoreErrorsAndContinue { get; set; }

        private string continuationFile;
        private bool useContinuationFile = false;

        public TimeSpan HeartbeatLatency { get; set; }

        public string ContinuationToken 
        {
            get { return continuationFile; }
            set
            {
                useContinuationFile = !string.IsNullOrWhiteSpace(value);
                continuationFile = value;
            }
        }
        public bool UseContinuationFile
        {
            get { return useContinuationFile; }
        }

        /// <summary>
        /// Limit total size of documents in each chunk
        /// </summary>
        public long TotalDocumentSizeInChunkLimitInBytes
        {
            get { return totalDocumentSizeInChunkLimitInBytes; }
            set
            {
                if (value < 1024)
                    throw new InvalidOperationException("Total document size in a chunk cannot be less than 1kb");

                totalDocumentSizeInChunkLimitInBytes = value;
            }
        }

        /// <summary>
        /// The number of documents to import before new connection will be opened.
        /// </summary>
        public int ChunkSize
        {
            get { return chunkSize; }
            set
            {
                if (value < 1)
                    throw new InvalidOperationException("Chunk size cannot be zero or a negative number");
                chunkSize = value;
            }
        }

        /// <summary>
        /// If this flag is true, during import of documents the smuggler won't use compression. False by default.
        /// </summary>
        public bool DisableCompressionOnImport { get; set; }

        public bool ExportDeletions { get; set; }

        /// <summary>
        /// Start exporting from the specified documents etag
        /// </summary>
        public Etag StartDocsEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified attachments etag
        /// </summary>
        [Obsolete("Use RavenFS instead.")]
        public Etag StartAttachmentsEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified document deletion etag
        /// </summary>
        public Etag StartDocsDeletionEtag { get; set; }

        /// <summary>
        /// Start exporting from the specified attachment deletion etag
        /// </summary>
        [Obsolete("Use RavenFS instead.")]
        public Etag StartAttachmentsDeletionEtag { get; set; }


        /// <summary>
        /// Specify the types to operate on. You can specify more than one type by combining items with the OR parameter.
        /// Default is all items.
        /// Usage example: OperateOnTypes = ItemType.Indexes | ItemType.Transformers | ItemType.Documents | ItemType.Attachments.
        /// </summary>
        public ItemType OperateOnTypes { get; set; }

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
                    matchedFilter |= filter.Values.Any(value => WildcardMatcher.Matches(value, val)) ==
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

        /// <summary>
        /// It allows to turn off versioning bundle for the duration of the import
        /// </summary>
        public bool ShouldDisableVersioningBundle { get; set; }

        /// <summary>
        /// When set ovverides the default document name.
        /// </summary>
        public string NoneDefaultFileName { get; set; }

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

        public string TransformScript { get; set; }

        /// <summary>
        /// Maximum number of steps that transform script can have
        /// </summary>
        public int MaxStepsForTransformScript { get; set; }

        public bool WaitForIndexing { get; set; }

        public bool StripReplicationInformation { get; set; }

        public bool SkipConflicted { get; set; }

        public int MaxSplitExportFileSize { get; set; }
    }

 

    [Flags]
    public enum ItemType
    {
        Documents = 0x1,
        Indexes = 0x2,
        [Obsolete("Use RavenFS instead.")]
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
