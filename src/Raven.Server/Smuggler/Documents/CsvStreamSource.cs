using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Raven.Client;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class CsvImportOptions 
    {
        public string Delimiter { get; set; }
        public char Quote { get; set; }
        public char? Comment { get; set; }
        public bool AllowComments { get; set; }
        public TrimOptions TrimOptions { get; set; }

        public CsvImportOptions()
        {
            Delimiter = ",";
            Quote = '"';
        }
        
        public CsvImportOptions(string delimiter, char quote, TrimOptions trim, bool allowComments, char comment)
        {
            Delimiter = delimiter;
            Quote = quote;
            TrimOptions = trim;
            Comment = comment;
            AllowComments = allowComments;
        }
    }
        
    public class CsvStreamSource : ISmugglerSource, IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly Stream _stream;
        private readonly DocumentsOperationContext _context;
        private SmugglerResult _result;
        private DatabaseItemType _currentType;
        private readonly string _collection;
        private StreamReader _reader;
        private CsvReader _csvReader;
        private readonly CsvConfiguration _csvHelperConfig;
        private bool _hasId;
        private int _idIndex;
        private bool _hasCollection;
        private int _collectionIndex;
        private readonly Dictionary<int, string> _metadataPositionToPropertyNames = new Dictionary<int, string>();

        /// <summary>
        /// This dictionary maps the index of a property to its nested segments.
        /// </summary>
        private Dictionary<int, string[]> _nestedPropertyDictionary;

        private bool _headersProcessed;
        private string[] _csvReaderFieldHeaders;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public CsvStreamSource(DocumentDatabase database, Stream stream, DocumentsOperationContext context, string collection, CsvImportOptions csvConfig)
        {
            _database = database;
            _stream = stream;
            _context = context;
            _currentType = DatabaseItemType.Documents;
            _collection = collection;
            
            _csvHelperConfig = new CsvConfiguration(CultureInfo.InvariantCulture);
            
            _csvHelperConfig.Delimiter = csvConfig.Delimiter;
            _csvHelperConfig.Quote = csvConfig.Quote;
            _csvHelperConfig.TrimOptions = csvConfig.TrimOptions;
            _csvHelperConfig.AllowComments = csvConfig.AllowComments;
            if (csvConfig.AllowComments && csvConfig.Comment != null)
            {
                _csvHelperConfig.Comment = (char)csvConfig.Comment;
            }
            
            _csvHelperConfig.BadDataFound = null;
        }

        public IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, out long buildVersion)
        {
            buildVersion = ServerVersion.DevBuildNumber;
            
            _reader = new StreamReader(_stream);
            _csvReader = new CsvReader(_reader, _csvHelperConfig);
            
            _result = result;
            
            return new DisposableAction(() =>
            {
                _reader.Dispose();
                _csvReader.Dispose();
            });
        }

        private bool ProcessFieldsIfNeeded()
        {
            if (_headersProcessed)
                return false;

            if (_csvReader.ReadHeader() == false)
                throw new InvalidOperationException("CSV file must contain a header row.");

            for (var i = 0; i < _csvReader.Context.HeaderRecord.Length; i++)
            {
                var property = _csvReader.Context.HeaderRecord[i];

                if (_csvReader.Context.HeaderRecord[i][0] == '@')
                {
                    if (property == Constants.Documents.Metadata.Id)
                    {
                        _hasId = true;
                        _idIndex = i;
                        continue;
                    }

                    if (property.StartsWith(Constants.Documents.Metadata.Key))
                    {
                        // metadata fields
                        var metadataProperty = property.Split(".").Last();
                        _metadataPositionToPropertyNames[i] = metadataProperty;

                        if (metadataProperty == Constants.Documents.Metadata.Collection)
                        {
                            _hasCollection = true;
                            _collectionIndex = i;
                        }

                        continue;
                    }
                }

                var indexOfDot = _csvReader.Context.HeaderRecord[i].IndexOf('.');
                //We probably have a nested property
                if (indexOfDot >= 0)
                {
                    if (_nestedPropertyDictionary == null)
                        _nestedPropertyDictionary = new Dictionary<int, string[]>();

                    var (arr, hasSegments) = SplitByDotWhileIgnoringEscapedDot(_csvReader.Context.HeaderRecord[i]);
                    //May be false if all dots are escaped
                    if (hasSegments)
                        _nestedPropertyDictionary[i] = arr;
                }
            }

            _csvReaderFieldHeaders = new string[_csvReader.Context.HeaderRecord.Length];
            _csvReader.Context.HeaderRecord.CopyTo(_csvReaderFieldHeaders, 0);

            _headersProcessed = true;
            return true;
        }

        private static (string[] Segments, bool HasSegments) SplitByDotWhileIgnoringEscapedDot(string csvReaderFieldHeader)
        {
            List<string> segments = new List<string>();
            bool escaped = false;
            int startSegment = 0;
            //Need to handle the general case where we can have Foo.'Foos.Name'.First
            for (var i = 0; i < csvReaderFieldHeader.Length; i++)
            {
                if (csvReaderFieldHeader[i] == '.' && escaped == false)
                {
                    segments.Add(csvReaderFieldHeader.Substring(startSegment, i - startSegment));
                    startSegment = i + 1;
                    continue;
                }
                if (csvReaderFieldHeader[i] == '\'')
                {
                    escaped ^= escaped;
                }
            }
            //No segments, this means the dot is escaped e.g. 'Foo.Bar'
            if (startSegment == 0)
            {
                return (null, false);
            }
            //Adding the last segment
            segments.Add(csvReaderFieldHeader.Substring(startSegment, csvReaderFieldHeader.Length - startSegment));
            //At this point we have at least 2 segments
            return (segments.ToArray(), true);
        }

        public DatabaseItemType GetNextType()
        {
            var type = _currentType;
            _currentType = DatabaseItemType.None;
            return type;
        }

        public DatabaseRecord GetDatabaseRecord()
        {
            return new DatabaseRecord();
        }

        public IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            var line = 0;
            while (_csvReader.Read())
            {
                line++;

                if (ProcessFieldsIfNeeded())
                    continue;

                var context = actions.GetContextForNewDocument();
                DocumentItem item;
                try
                {
                    item = ConvertRecordToDocumentItem(context, _csvReader.Context.Record, _csvReaderFieldHeaders, _collection);
                }
                catch (Exception e)
                {
                    _result.AddError($"Fail to parse CSV line {line}, Error:{e}");
                    _result.Documents.ErroredCount++;
                    continue;
                }
                yield return item;
            }

        }

        private DocumentItem ConvertRecordToDocumentItem(DocumentsOperationContext context, string[] csvReaderCurrentRecord, string[] csvReaderFieldHeaders, string collection)
        {
            try
            {
                var idStr = _hasId ? csvReaderCurrentRecord[_idIndex] : _hasCollection ? $"{csvReaderCurrentRecord[_collectionIndex]}/" : $"{collection}/";
                var data = new DynamicJsonValue();
                var metadata = new DynamicJsonValue();

                for (var i = 0; i < csvReaderFieldHeaders.Length; i++)
                {
                    if (csvReaderFieldHeaders[i][0] == '@')
                    {
                        if (_idIndex == i)
                            continue;

                        if (_metadataPositionToPropertyNames.TryGetValue(i, out var propertyName))
                        {
                            metadata[propertyName] = csvReaderCurrentRecord[i];
                            continue;
                        }
                    }

                    if (_nestedPropertyDictionary != null && _nestedPropertyDictionary.TryGetValue(i, out var segments))
                    {
                        var nestedData = data;
                        for (var j = 0; ; j++)
                        {
                            //last segment holds the data
                            if (j == segments.Length - 1)
                            {
                                nestedData[segments[j]] = ParseValue(csvReaderCurrentRecord[i]);
                                break; //we are done
                            }
                            //Creating the objects along the path if needed e.g. Foo.Bar.Name will create the 'Bar' object if needed
                            if (nestedData[segments[j]] == null)
                            {
                                var tmpRef = new DynamicJsonValue();
                                nestedData[segments[j]] = tmpRef;
                                nestedData = tmpRef;
                            }
                            //We need to advance into the nested object, since it is not the last segment it must be of type 'DynamicJsonValue'
                            else
                            {
                                nestedData = (DynamicJsonValue)nestedData[segments[j]];
                            }
                        }
                        continue;
                    }

                    data[csvReaderFieldHeaders[i]] = ParseValue(csvReaderCurrentRecord[i]);
                }

                if (_hasCollection == false)
                {
                    metadata[Constants.Documents.Metadata.Collection] = collection;
                }

                data[Constants.Documents.Metadata.Key] = metadata;

                return new DocumentItem
                {
                    Document = new Document
                    {
                        Data = context.ReadObject(data, idStr),
                        Id = context.GetLazyString(idStr),
                        ChangeVector = string.Empty,
                        Flags = DocumentFlags.None,
                        NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler,
                        LastModified = _database.Time.GetUtcNow(),
                    },
                    Attachments = null
                };
            }
            finally
            {
                foreach (var disposeMe in _disposables)
                {
                    disposeMe.Dispose();
                }
                _disposables.Clear();
            }
        }

        private object ParseValue(string s)
        {
            if (string.IsNullOrEmpty(s))
                return null;
            if (s.StartsWith('\"') && s.EndsWith('\"'))
                return s.Substring(1, s.Length - 2);
            if (s.StartsWith('[') && s.EndsWith(']'))
            {
                var array = _context.ParseBufferToArray(s, "CsvImport/ArrayValue",
                    BlittableJsonDocumentBuilder.UsageMode.None);
                _disposables.Add(array);
                return array;
            }
            if (char.IsDigit(s[0]) || s[0] == '-')
            {
                if (s.IndexOf('.') > 0)
                {
                    if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var dec))
                        return dec;
                }
                else
                {
                    if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
                        return l;
                }
            }

            if ((s.Length == 4 && s[0] == 't' || s.Length == 5 && s[0] == 'f') && bool.TryParse(s, out var b))
            {
                return b;
            }

            if (s.Length == 4 && s.Equals("null"))
                return null;

            return s;
        }

        public IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return Enumerable.Empty<DocumentItem>();
        }

        public IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions)
        {
            return Enumerable.Empty<DocumentItem>();
        }

        public IEnumerable<string> GetLegacyAttachmentDeletions()
        {
            return Enumerable.Empty<string>();
        }

        public IEnumerable<string> GetLegacyDocumentDeletions()
        {
            return Enumerable.Empty<string>();
        }

        public IEnumerable<Tombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions)
        {
            return Enumerable.Empty<Tombstone>();
        }

        public IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions)
        {
            yield break;
        }

        public IEnumerable<IndexDefinitionAndType> GetIndexes()
        {
            return Enumerable.Empty<IndexDefinitionAndType>();
        }

        public IEnumerable<(string Prefix, long Value, long Index)> GetIdentities()
        {
            return Enumerable.Empty<(string Prefix, long Value, long Index)>();
        }

        public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValues(INewCompareExchangeActions actions)
        {
            return Enumerable.Empty<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)>();
        }

        public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstones()
        {
            return Enumerable.Empty<(CompareExchangeKey Key, long Index)>();
        }

        public IEnumerable<CounterGroupDetail> GetCounterValues(List<string> collectionsToExport, ICounterActions actions)
        {
            return Enumerable.Empty<CounterGroupDetail>();
        }

        public IEnumerable<CounterDetail> GetLegacyCounterValues()
        {
            return Enumerable.Empty<CounterDetail>();
        }

        public IEnumerable<SubscriptionState> GetSubscriptions()
        {
            return Enumerable.Empty<SubscriptionState>();
        }

        public IEnumerable<TimeSeriesItem> GetTimeSeries(List<string> collectionsToExport)
        {
            return Enumerable.Empty<TimeSeriesItem>();
        }

        public long SkipType(DatabaseItemType type, Action<long> onSkipped, CancellationToken token)
        {
            return 0;
        }

        public SmugglerSourceType GetSourceType()
        {
            return SmugglerSourceType.Import;
        }

        public void Dispose()
        {
        }
    }
}
