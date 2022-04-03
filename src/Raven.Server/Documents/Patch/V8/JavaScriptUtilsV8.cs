using System;
using System.Collections.Generic;
using V8.Net;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Sparrow.Json;
using Raven.Server.Extensions.V8;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch.V8
{
    public class JavaScriptUtilsV8 : JavaScriptUtilsBase
    {
        public readonly V8EngineEx EngineEx;
        public readonly V8Engine Engine;

        public JavaScriptUtilsV8(ScriptRunner runner, V8EngineEx engine)
            : base(runner, engine)
        {
            EngineEx = engine;
            Engine = engine;
        }

        public InternalHandle GetMetadata(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            if (args.Length != 1 && args.Length != 2 || //length == 2 takes into account Query Arguments that can be added to args 
                !(args[0].BoundObject is BlittableObjectInstanceV8 boi))
                throw new InvalidOperationException("metadataFor(doc) must be called with a single entity argument");

            return boi.GetMetadata(toReturnCopy: true);
        }

        internal InternalHandle AttachmentsFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            var engineEx = (V8EngineEx)engine;
            if (args.Length != 1 || !(args[0].BoundObject is BlittableObjectInstanceV8 boi))
                throw new InvalidOperationException($"{nameof(AttachmentsFor)} must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return EmptyArray(engine);

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return EmptyArray(engine);

            int arrayLength =  attachments.Length;
            InternalHandle[] jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; i++) 
            {
                var anoi = new AttachmentNameObjectInstanceV8(EngineEx, (BlittableJsonReaderObject)attachments[i]);
                jsItems[i] = anoi.CreateObjectBinder(keepAlive: true);
            }

            return engineEx.CreateArrayWithDisposal(jsItems);

            static InternalHandle EmptyArray(V8Engine engine)
            {
                return engine.CreateArray(Array.Empty<InternalHandle>());
            }
        }

        internal static InternalHandle LoadAttachment(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            var engineEx = (V8EngineEx)engine;

            if (args.Length != 2)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with two arguments, but '{args.Length}' were passed.");

            if (args[0].IsNull)
                return engineEx.Context.ImplicitNullV8().Clone();

            if (args[0].IsObject == false)
                ThrowInvalidFirstParameter();

            var doc = args[0].BoundObject as BlittableObjectInstanceV8;
            if (doc == null)
                ThrowInvalidFirstParameter();

            if (args[1].IsStringEx == false)
                ThrowInvalidSecondParameter();

            var attachmentName = args[1].AsString;

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            var attachment = CurrentIndexingScope.Current.LoadAttachment(doc.DocumentId, attachmentName);
            if (attachment is DynamicNullObject)
                return engineEx.Context.ImplicitNullV8().Clone();

            var aoi = new AttachmentObjectInstanceV8(engineEx, (DynamicAttachment)attachment);
            return aoi.CreateObjectBinder(keepAlive: true);

            void ThrowInvalidFirstParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            void ThrowInvalidSecondParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {args[1].GetType().FullName}.");
            }
        }

        internal static InternalHandle LoadAttachments(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            if (args.Length != 1)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with one argument, but '{args.Length}' were passed.");

            var engineEx = (V8EngineEx)engine;
            InternalHandle jsRes = InternalHandle.Empty;
            if (args[0].IsNull)
                return engineEx.Context.ImplicitNullV8().Clone();

            if (!(args[0].BoundObject is BlittableObjectInstanceV8 doc))
                ThrowInvalidParameter();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            var attachments = CurrentIndexingScope.Current.LoadAttachments(doc.DocumentId, GetAttachmentNames());
            if (attachments.Count == 0)
                return engine.CreateArray(Array.Empty<InternalHandle>());

            int arrayLength =  attachments.Count;
            var jsItems = new InternalHandle[attachments.Count];
            for (int i = 0; i < arrayLength; i++)
            {
                var aoi = new AttachmentObjectInstanceV8(engineEx, (DynamicAttachment)attachments[i]);
                jsItems[i] = aoi.CreateObjectBinder(keepAlive: true);
            }

            return engineEx.CreateArrayWithDisposal(jsItems);

            void ThrowInvalidParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            IEnumerable<string> GetAttachmentNames()
            {
                using (var jsMetadata = args[0].GetProperty(Constants.Documents.Metadata.Key))
                {
                    var metadata = jsMetadata.BoundObject as BlittableObjectInstanceV8;
                    if (metadata == null)
                        yield break;

                    using (var jsAttachments = jsMetadata.GetProperty(Constants.Documents.Metadata.Attachments))
                    {
                        if (jsAttachments.IsArray == false)
                            yield break;

                        int arrayLength =  jsAttachments.ArrayLength;
                        for (int i = 0; i < arrayLength; i++)
                        {
                            using (var jsAttachment = jsAttachments.GetProperty(i))
                                yield return jsAttachment.GetProperty(nameof(AttachmentName.Name)).AsString;
                        }
                    }
                }
            }
        }

        internal static InternalHandle GetTimeSeriesNamesFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            return GetNamesFor(engine, isConstructCall, self, args, Constants.Documents.Metadata.TimeSeries, "timeSeriesNamesFor");
        }

        internal static InternalHandle GetCounterNamesFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            return GetNamesFor(engine, isConstructCall, self, args, Constants.Documents.Metadata.Counters, "counterNamesFor");
        }

        private static InternalHandle GetNamesFor(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args, string metadataKey, string methodName)
        {
            if (args.Length != 1 || !(args[0].BoundObject is BlittableObjectInstanceV8 boi))
                throw new InvalidOperationException($"{methodName}(doc) must be called with a single entity argument");

            var engineEx = (V8EngineEx)engine;
            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return engine.CreateArray(Array.Empty<InternalHandle>());

            if (metadata.TryGet(metadataKey, out BlittableJsonReaderArray timeSeries) == false)
                return engine.CreateArray(Array.Empty<InternalHandle>());

            InternalHandle[] jsItems = new InternalHandle[timeSeries.Length];
            for (var i = 0; i < timeSeries.Length; i++)
                jsItems[i] = engine.CreateValue(timeSeries[i]?.ToString());

            return engineEx.CreateArrayWithDisposal(jsItems);
        }

        public InternalHandle GetDocumentId(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            if (args.Length != 1 && args.Length != 2) //length == 2 takes into account Query Arguments that can be added to args
                throw new InvalidOperationException("id(doc) must be called with a single argument");

            InternalHandle jsDoc = args[0];
            if (jsDoc.IsNull || jsDoc.IsUndefined)
                return jsDoc;

            if (jsDoc.IsObject == false)
                throw new InvalidOperationException("id(doc) must be called with an object argument");

            if (jsDoc.BoundObject is BlittableObjectInstanceV8 doc && doc.DocumentId != null)
                return engine.CreateValue(doc.DocumentId);

            //throw new InvalidOperationException("jsDoc is not BoundObject");
            using (var jsValue = jsDoc.GetProperty(Constants.Documents.Metadata.Key))
            {
                // search either @metadata.@id or @id
                using (var metadata = jsValue.IsObject == false ? jsDoc : jsValue)
                {
                    var jsRes = metadata.GetProperty(Constants.Documents.Metadata.Id);
                    if (jsRes.IsStringEx == false)
                    {
                        // search either @metadata.Id or Id
                        jsRes.Dispose();
                        jsRes = metadata.GetProperty(Constants.Documents.Metadata.IdProperty);
                        if (jsRes.IsStringEx == false)
                        {
                            jsRes.Dispose();
                            return engine.CreateNullValue();
                        }
                    }
                    return jsRes;
                }
            }
        }

        public InternalHandle TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false, BlittableObjectInstanceV8 parent = null)
        {
            if (o is TimeSeriesRetriever.TimeSeriesStreamingRetrieverResult tsrr)
            {
                // we are passing a streaming value to the JS engine, so we need
                // to materialize all the results
                
                
                var results = new DynamicJsonArray(tsrr.Stream);
                var djv = new DynamicJsonValue
                {
                    [nameof(TimeSeriesAggregationResult.Count)] = results.Count,
                    [nameof(TimeSeriesAggregationResult.Results)] = results
                };
                var boi = new BlittableObjectInstanceV8(this, null, context.ReadObject(djv, "MaterializedStreamResults"), null, null, null);
                return boi.CreateObjectBinder(keepAlive);
            }
            if (o is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?, ProjectionOptions> t)
            {
                var d = t.Item1;
                var boi = new BlittableObjectInstanceV8(this, parent, Clone(d.Data, context), d)
                {
                    LuceneDocument = t.Item2,
                    LuceneState = t.Item3,
                    LuceneIndexFields = t.Item4,
                    LuceneAnyDynamicIndexFields = t.Item5 ?? false,
                    Projection = t.Item6
                };
                return boi.CreateObjectBinder(keepAlive);
            }
            if (o is Document doc)
            {
                BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(this, parent, Clone(doc.Data, context), doc);
                return boi.CreateObjectBinder(keepAlive);
            }
            if (o is DocumentConflict dc)
            {
                BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(this, parent, Clone(dc.Doc, context), dc.Id, dc.LastModified, dc.ChangeVector);
                return boi.CreateObjectBinder(keepAlive);
            }

            if (o is BlittableJsonReaderObject json)
            {
                BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(this, parent, Clone(json, context), null, null, null);
                return boi.CreateObjectBinder(keepAlive);
            }

            if (o == null)
                return Engine.CreateNullValue();
            if (o is long lng)
                return Engine.CreateValue(lng);
            if (o is BlittableJsonReaderArray bjra)
            {
                int arrayLength =  bjra.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, bjra[i]);
                    if (jsItems[i].IsError)
                        return jsItems[i];
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            if (o is List<object> list)
            {
                int arrayLength = list.Count;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, list[i]);
                    if (jsItems[i].IsError)
                        return jsItems[i];
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            if (o is List<Document> docList)
            {
                int arrayLength =  docList.Count;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(this, parent, Clone(docList[i].Data, context), docList[i]);
                    jsItems[i] = boi.CreateObjectBinder(keepAlive);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            // for admin
            if (o is RavenServer || o is DocumentDatabase)
            {
                AssertAdminScriptInstance();
                return Engine.FromObject(o);
            }
            if (o is V8NativeObject j)
            {
                InternalHandle h = j._;
                return new InternalHandle(ref h, true);
            }
            if (o is bool b)
                return Engine.CreateValue(b);
            if (o is int integer)
                return Engine.CreateValue(integer);
            if (o is double dbl)
                return Engine.CreateValue(dbl);
            if (o is string s)
                return Engine.CreateValue(s);
            if (o is LazyStringValue ls)
                return Engine.CreateValue(ls.ToString());
            if (o is LazyCompressedStringValue lcs)
                return Engine.CreateValue(lcs.ToString());
            if (o is LazyNumberValue lnv)
            {
                return BlittableObjectInstanceV8.BlittableObjectProperty.GetJsValueForLazyNumber(EngineEx, lnv);
            }
            if (o is InternalHandle js)
                return js;
            throw new InvalidOperationException("No idea how to convert " + o + " to InternalHandle");
        }

    }
}
