using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.Counters.V8;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.TimeSeries.V8;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{
    public class JavaScriptUtilsV8 : JavaScriptUtilsBase<JsHandleV8>
    {
        public readonly V8Engine Engine;
        public readonly V8EngineEx EngineEx;
        public JavaScriptUtilsV8(ScriptRunnerV8 runner, V8EngineEx engine)
            : base(runner, engine)
        {
            EngineEx = engine;
            Engine = engine.Engine;
        }

        public static JavaScriptUtilsV8 Create(ScriptRunnerV8 runner, V8EngineEx engine)
        {
            return new JavaScriptUtilsV8(runner, engine);
        }

        public override JsHandleV8 GetMetadata(JsHandleV8 self, JsHandleV8[] args)
        {
            if (args.Length != 1 && args.Length != 2 || //length == 2 takes into account Query Arguments that can be added to args 
                args[0].AsObject() is BlittableObjectInstanceV8 boi == false)
                throw new InvalidOperationException("metadataFor(doc) must be called with a single entity argument");

            return boi.GetMetadata();
        }

        public override JsHandleV8 AttachmentsFor(JsHandleV8 self, JsHandleV8[] args)
        {
            if (args.Length != 1 || args[0].AsObject() is BlittableObjectInstanceV8 boi == false)
                throw new InvalidOperationException($"{nameof(AttachmentsFor)} must be called with a single entity argument");
            var handle = (V8EngineEx)EngineHandle;
            if (boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata == false)
                return handle.CreateEmptyArray();

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return handle.CreateEmptyArray();

            int arrayLength = attachments.Length;
            InternalHandle[] jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; i++)
            {
                //TODO: egor fix cast
                var anoi = new AttachmentNameObjectInstanceV8(handle, (BlittableJsonReaderObject)attachments[i]);
                jsItems[i] = anoi.CreateObjectBinder(keepAlive: true);
            }
            var arr = Engine.CreateArrayWithDisposal(jsItems);
            return new JsHandleV8(ref arr);
        }

        public override JsHandleV8 LoadAttachment(JsHandleV8 self, params JsHandleV8[] args)
        {
            if (args.Length != 2)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with two arguments, but '{args.Length}' were passed.");
            var handle = (V8EngineEx)EngineHandle;
            if (args[0].IsNull)
                return handle.ImplicitNull();

            if (args[0].IsObject == false)
                ThrowInvalidFirstParameter();

            var doc = args[0].AsObject() as BlittableObjectInstanceV8;
            if (doc == null)
                ThrowInvalidFirstParameter();

            if (args[1].IsStringEx == false)
                ThrowInvalidSecondParameter();

            var attachmentName = args[1].AsString;

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            var attachment = CurrentIndexingScope.Current.LoadAttachment(doc.DocumentId, attachmentName);
            if (attachment is DynamicNullObject)
                return handle.ImplicitNull();

            var aoi = new AttachmentObjectInstanceV8(handle, (DynamicAttachment)attachment);
            var obj = aoi.CreateObjectBinder(keepAlive: true);
            return new JsHandleV8(ref obj);

            void ThrowInvalidFirstParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            void ThrowInvalidSecondParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {args[1].GetType().FullName}.");
            }
        }

        public override JsHandleV8 LoadAttachments(JsHandleV8 self, params JsHandleV8[] args)
        {
            if (args.Length != 1)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with one argument, but '{args.Length}' were passed.");

            var handle = (V8EngineEx)EngineHandle;
            if (args[0].IsNull)
                return handle.ImplicitNull();

            if (!(args[0].AsObject() is BlittableObjectInstanceV8 doc))
                ThrowInvalidParameter();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            var attachments = CurrentIndexingScope.Current.LoadAttachments(doc.DocumentId, GetAttachmentNames());
            if (attachments.Count == 0)
                return handle.CreateArray(Array.Empty<JsHandleV8>());

            int arrayLength = attachments.Count;
            var jsItems = new InternalHandle[attachments.Count];
            for (int i = 0; i < arrayLength; i++)
            {
                var aoi = new AttachmentObjectInstanceV8(handle, (DynamicAttachment)attachments[i]);
                jsItems[i] = aoi.CreateObjectBinder(keepAlive: true);
            }

            return handle.CreateArray(jsItems.ToJsHandleArray());

            void ThrowInvalidParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            IEnumerable<string> GetAttachmentNames()
            {
                using (var jsMetadata = args[0].GetProperty(Constants.Documents.Metadata.Key))
                {
                    var metadata = jsMetadata.AsObject() as BlittableObjectInstanceV8;
                    if (metadata == null)
                        yield break;

                    using (var jsAttachments = jsMetadata.GetProperty(Constants.Documents.Metadata.Attachments))
                    {
                        if (jsAttachments.IsArray == false)
                            yield break;

                        int arrayLength = jsAttachments.ArrayLength;
                        for (int i = 0; i < arrayLength; i++)
                        {
                            using (var jsAttachment = jsAttachments.GetProperty(i))
                                yield return jsAttachment.GetProperty(nameof(AttachmentName.Name)).AsString;
                        }
                    }
                }
            }
        }

        public override JsHandleV8 GetTimeSeriesNamesFor(JsHandleV8 self, JsHandleV8[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.TimeSeries, "timeSeriesNamesFor");
        }

        public override JsHandleV8 GetCounterNamesFor(JsHandleV8 self, JsHandleV8[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.Counters, "counterNamesFor");
        }

        public JsHandleV8 GetNamesFor(JsHandleV8 self, JsHandleV8[] args, string metadataKey, string methodName)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstanceV8 boi))
                throw new InvalidOperationException($"{methodName}(doc) must be called with a single entity argument");

            var handle = (V8EngineEx)EngineHandle;
            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return handle.CreateEmptyArray();

            if (metadata.TryGet(metadataKey, out BlittableJsonReaderArray timeSeries) == false)
                return handle.CreateEmptyArray();

            InternalHandle[] jsItems = new InternalHandle[timeSeries.Length];
            for (var i = 0; i < timeSeries.Length; i++)
                jsItems[i] = Engine.CreateValue(timeSeries[i]?.ToString());

            return handle.CreateArray(jsItems.ToJsHandleArray());
        }

        public override JsHandleV8 GetDocumentId(JsHandleV8 self, JsHandleV8[] args)
        {
            if (args.Length != 1 && args.Length != 2) //length == 2 takes into account Query Arguments that can be added to args
                throw new InvalidOperationException("id(doc) must be called with a single argument");

            JsHandleV8 jsDoc = args[0];
            if (jsDoc.IsNull || jsDoc.IsUndefined)
                return jsDoc;

            if (jsDoc.IsObject == false)
                throw new InvalidOperationException("id(doc) must be called with an object argument");

            var handle = (V8EngineEx)EngineHandle;
            if (jsDoc.AsObject() is BlittableObjectInstanceV8 doc && doc.DocumentId != null)
            {
                var res = handle.CreateValue(doc.DocumentId);
                return res;
            }

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
                            return handle.CreateNullValue();
                        }
                    }
                    return jsRes;
                }
            }
        }

        public override IBlittableObjectInstance CreateBlittableObjectInstanceFromScratch(IBlittableObjectInstance parent, BlittableJsonReaderObject blittable,
            string id, DateTime? lastModified, string changeVector)
        {
            return new BlittableObjectInstanceV8(this, (BlittableObjectInstanceV8)parent, blittable, id, lastModified, changeVector);
        }

        public override IBlittableObjectInstance CreateBlittableObjectInstanceFromDoc(IBlittableObjectInstance parent, BlittableJsonReaderObject blittable, Document doc)
        {
            return new BlittableObjectInstanceV8(this, (BlittableObjectInstanceV8)parent, blittable, doc);
        }

        public override IObjectInstance<JsHandleV8> CreateTimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment)
        {
            return new TimeSeriesSegmentObjectInstanceV8(EngineEx, segment);
        }

        public override IObjectInstance<JsHandleV8> CreateCounterEntryObjectInstance(DynamicCounterEntry entry)
        {
            return new CounterEntryObjectInstanceV8(EngineEx, entry);
        }

        public override JsHandleV8 TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false)
        {
            return TranslateToJs(context, o, keepAlive, parent: null);
        }

        public JsHandleV8 TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false, BlittableObjectInstanceV8 parent = null)
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
                return EngineEx.CreateNullValue();
            if (o is long lng)
                return EngineEx.CreateValue(lng);
            if (o is BlittableJsonReaderArray bjra)
            {
                int arrayLength = bjra.Length;
                var jsItems = new JsHandleV8[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, bjra[i]);
                    if (jsItems[i].IsError)
                        return jsItems[i];
                }

                return EngineEx.CreateArray(jsItems);
            }
            if (o is List<object> list)
            {
                int arrayLength = list.Count;
                var jsItems = new JsHandleV8[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, list[i]);
                    if (jsItems[i].IsError)
                        return jsItems[i];
                }

                return EngineEx.CreateArray(jsItems);
            }
            if (o is List<Document> docList)
            {
                int arrayLength = docList.Count;
                var jsItems = new JsHandleV8[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    BlittableObjectInstanceV8 boi = new BlittableObjectInstanceV8(this, parent, Clone(docList[i].Data, context), docList[i]);
                    jsItems[i] = boi.CreateObjectBinder(keepAlive);
                }

                return EngineEx.CreateArray(jsItems);
            }
            // for admin
            if (o is RavenServer || o is DocumentDatabase)
            {
                AssertAdminScriptInstance();
                return EngineEx.FromObjectGen(o);
            }
            if (o is V8NativeObject j)
            {
                var h = j._;
                return new JsHandleV8(ref h);
            }
            if (o is bool b)
                return EngineEx.CreateValue(b);
            if (o is int integer)
                return EngineEx.CreateValue(integer);
            if (o is double dbl)
                return EngineEx.CreateValue(dbl);
            if (o is string s)
                return EngineEx.CreateValue(s);
            if (o is LazyStringValue ls)
                return EngineEx.CreateValue(ls.ToString());
            if (o is LazyCompressedStringValue lcs)
                return EngineEx.CreateValue(lcs.ToString());
            if (o is LazyNumberValue lnv)
            {
                var ih = BlittableObjectInstanceV8.BlittableObjectProperty.GetJsValueForLazyNumber(EngineEx, lnv);
                return new JsHandleV8(ref ih);
            }
            if (o is JsHandleV8 js)
                return js;
            throw new InvalidOperationException("No idea how to convert " + o + " to JsHandleV8");
        }

    }
}
