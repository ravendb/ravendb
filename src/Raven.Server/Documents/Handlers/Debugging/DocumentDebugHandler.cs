using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DocumentDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/huge", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                writer.WriteStartArray();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName("Id");
                    writer.WriteString(pair.Key.Item1);

                    writer.WriteComma();

                    writer.WritePropertyName("Size");
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName("LastAccess");
                    writer.WriteString(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/debug/documents/scan-corrupted-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ScanCorruptedIds()
        {
            var startEtag = GetIntValueQueryString("startEtag", required: false) ?? 0;
            var resultCount = GetIntValueQueryString("resultCount", required: false) ?? 1024;
            var maxBatchTimeInSec = GetIntValueQueryString("maxBatchTimeInSec", required: false) ?? 60;

            var wrongEscapedPositionsIds = new List<string>();
            var unescapedControlCharacterIds = new List<string>();
            long lastEtag = startEtag;
            var scannedDocuments = 0;
            var maxBatchTime = TimeSpan.FromSeconds(maxBatchTimeInSec);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null))
                {
                    var timeout = Stopwatch.StartNew();
                    while (resultCount > 0)
                    {
                        timeout.Restart();
                        using (context.OpenReadTransaction())
                        {
                            foreach (var doc in IterateDocumentsAndRevisionsByEtag(context, lastEtag))
                            {
                                HttpContext.RequestAborted.ThrowIfCancellationRequested();

                                ++scannedDocuments;
                                using (doc)
                                {
                                    unsafe
                                    {
                                        using var cloned = context.GetLazyString(doc.LowerId.Buffer, doc.LowerId.Size, longLived: false);
                                        if (cloned.Length > doc.LowerId.Length)
                                        {
                                            AddToResult(unescapedControlCharacterIds, doc);
                                            resultCount--;
                                        }
                                    }

                                    try
                                    {
                                        writer.WriteString(doc.Id);
                                    }
                                    catch (Exception)
                                    {
                                        AddToResult(wrongEscapedPositionsIds, doc);
                                        resultCount--;
                                    }

                                    if (resultCount <= 0 || timeout.Elapsed > maxBatchTime)
                                    {
                                        lastEtag = doc.Etag;
                                        break;
                                    }
                                }
                            }

                            break;
                        }
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("WrongEscapedPositionsIds", wrongEscapedPositionsIds);
                    writer.WriteComma();
                    writer.WriteArray("UnescapedControlCharacterIds", unescapedControlCharacterIds);
                    writer.WriteComma();
                    if (lastEtag > 0)
                    {
                        writer.WritePropertyName("LastEtag");
                        writer.WriteInteger(lastEtag);
                    }
                    else
                    {
                        writer.WritePropertyName("AllScanned");
                        writer.WriteBool(true);
                    }

                    writer.WriteComma();
                    writer.WritePropertyName("ScannedDocuments");
                    writer.WriteInteger(scannedDocuments);
                    writer.WriteEndObject();
                }
            }

            return;

            void AddToResult(List<string> list, Document doc)
            {
                list.Add(
                    $"Id: '{doc.Id}', LowerId: '{doc.LowerId}', ChangeVector: '{doc.ChangeVector}', Etag: '{doc.Etag}', Flags: '{doc.Flags}'");
            }
        }

        private IEnumerable<Document> IterateDocumentsAndRevisionsByEtag(DocumentsOperationContext context, long startEtag)
        {
            const DocumentFields documentFields = DocumentFields.Id | DocumentFields.LowerId | DocumentFields.ChangeVector;
            var documents = Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, long.MaxValue, documentFields);
            var revisions = Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, startEtag, long.MaxValue, documentFields);

            using var documentsEnumerator = documents.GetEnumerator();
            using var revisionsEnumerator = revisions.GetEnumerator();

            var mergedEnumerator = new MergedEnumerator<Document>(DocumentsEtagComparer.Instance);
            mergedEnumerator.AddEnumerator(documentsEnumerator);
            mergedEnumerator.AddEnumerator(revisionsEnumerator);
            
            while (mergedEnumerator.MoveNext())
            {
                yield return mergedEnumerator.Current;
            }
        }

        private class MergedEnumerator<T> : IEnumerator<T>
        {
            private readonly IComparer<T> _comparer;
            private readonly List<IEnumerator<T>> _workEnumerators = new();
            private T _currentItem;

            private IEnumerator<T> _currentEnumerator;

            public MergedEnumerator(IComparer<T> comparer)
            {
                _comparer = comparer;
            }

            public void AddEnumerator(IEnumerator<T> enumerator)
            {
                if (enumerator == null)
                    return;

                if (enumerator.MoveNext())
                {
                    _workEnumerators.Add(enumerator);
                }
                else
                {
                    enumerator.Dispose();
                }
            }

            public bool MoveNext()
            {
                if (_currentEnumerator != null)
                {
                    if (_currentEnumerator.MoveNext() == false)
                    {
                        using (_currentEnumerator)
                        {
                            _workEnumerators.Remove(_currentEnumerator);
                            _currentEnumerator = null;
                        }
                    }
                }

                if (_workEnumerators.Count == 0)
                    return false;

                _currentEnumerator = _workEnumerators[0];
                for (var index = 1; index < _workEnumerators.Count; index++)
                {
                    if (_comparer.Compare(_workEnumerators[index].Current, _currentEnumerator.Current) < 0)
                    {
                        _currentEnumerator = _workEnumerators[index];
                    }
                }

                _currentItem = _currentEnumerator.Current;

                return true;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            object IEnumerator.Current => Current;

            public T Current => _currentItem;

            public void Dispose()
            {
                foreach (var workEnumerator in _workEnumerators)
                {
                    workEnumerator.Dispose();
                }

                _workEnumerators.Clear();
            }
        }
        
        private class DocumentsEtagComparer : IComparer<Document>
        {
            public static readonly DocumentsEtagComparer Instance = new();

            private DocumentsEtagComparer()
            {
            }

            public int Compare(Document x, Document y) => x.Etag.CompareTo(y.Etag);
        }
    }
}
