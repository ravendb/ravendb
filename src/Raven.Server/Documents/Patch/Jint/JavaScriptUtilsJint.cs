using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.Counters.Jint;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.TimeSeries.Jint;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch.Jint
{
    public class JavaScriptUtilsJint : JavaScriptUtilsBase<JsHandleJint>
    {
        public readonly Engine Engine;
        public readonly JintEngineEx EngineEx;

        public JavaScriptUtilsJint(ScriptRunnerJint runner, JintEngineEx engine)
            : base(runner, engine)
        {
            Engine = engine.Engine;
            EngineEx = engine;
        }

        public static JavaScriptUtilsJint Create(ScriptRunnerJint runner, JintEngineEx engine)
        {
            return new JavaScriptUtilsJint(runner, engine);
        }

        public override JsHandleJint GetMetadata(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length != 1 && args.Length != 2 || //length == 2 takes into account Query Arguments that can be added to args
                !(args[0].AsObject() is BlittableObjectInstanceJint boi))
                throw new InvalidOperationException("metadataFor(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return new JsHandleJint(JsValue.Null);
            metadata.Modifications = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.ChangeVector] = boi.ChangeVector,
                [Constants.Documents.Metadata.Id] = boi.DocumentId,
                [Constants.Documents.Metadata.LastModified] = boi.LastModified,
            };

            if (boi.IndexScore != null)
                metadata.Modifications[Constants.Documents.Metadata.IndexScore] = boi.IndexScore.Value;

            if (boi.Distance != null)
                metadata.Modifications[Constants.Documents.Metadata.SpatialResult] = boi.Distance.Value.ToJson();

            // we cannot dispose the metadata here because the BOI is accessing blittable directly using the .Blittable property
            //using (var old = metadata)
            {
                metadata = Context.ReadObject(metadata, boi.DocumentId);
                JsHandleJint metadataJs = TranslateToJs(Context, metadata);
                boi.Set(new JsString(Constants.Documents.Metadata.Key), metadataJs.Item);

                return metadataJs;
            }
        }

        public override JsHandleJint AttachmentsFor(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstanceJint boi))
                throw new InvalidOperationException($"{nameof(AttachmentsFor)} must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return new JsHandleJint(EmptyArray(Engine));

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return new JsHandleJint(EmptyArray(Engine));

            JsValue[] attachmentsArray = new JsValue[attachments.Length];
            for (var i = 0; i < attachments.Length; i++)
                attachmentsArray[i] = new AttachmentNameObjectInstanceJint(Engine, (BlittableJsonReaderObject)attachments[i]);

            return new JsHandleJint(Engine.Realm.Intrinsics.Array.Construct(attachmentsArray));

            static ArrayInstance EmptyArray(Engine engine)
            {
                return engine.Realm.Intrinsics.Array.Construct(0);
            }
        }

        public override JsHandleJint LoadAttachment(JsHandleJint JsHandleJint, JsHandleJint[] args)
        {
            if (args.Length != 2)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with two arguments, but '{args.Length}' were passed.");

            if (args[0].IsNull)
                return new JsHandleJint(DynamicJsNullJint.ImplicitNullJint);

            if (args[0].IsObject == false)
                ThrowInvalidFirstParameter();

            var doc = args[0].AsObject() as BlittableObjectInstanceJint;
            if (doc == null)
                ThrowInvalidFirstParameter();

            if (args[1].IsString == false)
                ThrowInvalidSecondParameter();

            var attachmentName = args[1].AsString;

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            var attachment = CurrentIndexingScope.Current.LoadAttachment(doc.DocumentId, attachmentName);
            if (attachment is DynamicNullObject)
                return new JsHandleJint(DynamicJsNullJint.ImplicitNullJint);

            return new JsHandleJint(new AttachmentObjectInstanceJint(Engine, (DynamicAttachment)attachment));

            void ThrowInvalidFirstParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            void ThrowInvalidSecondParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {args[1].GetType().FullName}.");
            }
        }

        public override JsHandleJint LoadAttachments(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length != 1)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with one argument, but '{args.Length}' were passed.");

            if (args[0].IsNull)
                return new JsHandleJint(DynamicJsNullJint.ImplicitNullJint);

            if (args[0].IsObject == false)
                ThrowInvalidParameter();

            var doc = args[0].AsObject() as BlittableObjectInstanceJint;
            if (doc == null)
                ThrowInvalidParameter();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            var attachments = CurrentIndexingScope.Current.LoadAttachments(doc.DocumentId, GetAttachmentNames());
            if (attachments.Count == 0)
                return new JsHandleJint(EmptyArray(Engine));

            var values = new JsValue[attachments.Count];
            for (var i = 0; i < values.Length; i++)
                values[i] = new AttachmentObjectInstanceJint(Engine, attachments[i]);

            var array = Engine.Realm.Intrinsics.Array.Construct(Arguments.Empty);
            Engine.Realm.Intrinsics.Array.PrototypeObject.Push(array, values);

            return new JsHandleJint(array);

            void ThrowInvalidParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            IEnumerable<string> GetAttachmentNames()
            {
                var metadata = doc.Get(Constants.Documents.Metadata.Key) as BlittableObjectInstanceJint;
                if (metadata == null)
                    yield break;

                var attachments = metadata.Get(Constants.Documents.Metadata.Attachments);
                if (attachments == null || attachments.IsArray() == false)
                    yield break;

                foreach (var attachment in attachments.AsArray())
                {
                    var attachmentObject = attachment.AsObject();
                    yield return attachment.Get(nameof(AttachmentName.Name)).AsString();
                }
            }

            static ArrayInstance EmptyArray(Engine engine)
            {
                return engine.Realm.Intrinsics.Array.Construct(0);
            }
        }

        public override JsHandleJint GetTimeSeriesNamesFor(JsHandleJint self, JsHandleJint[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.TimeSeries, "timeSeriesNamesFor");
        }

        public override JsHandleJint GetCounterNamesFor(JsHandleJint self, JsHandleJint[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.Counters, "counterNamesFor");
        }

        private JsHandleJint GetNamesFor(JsHandleJint self, JsHandleJint[] args, string metadataKey, string methodName)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstanceJint boi))
                throw new InvalidOperationException($"{methodName}(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return new JsHandleJint(EmptyArray(Engine));

            if (metadata.TryGet(metadataKey, out BlittableJsonReaderArray timeSeries) == false)
                return new JsHandleJint(EmptyArray(Engine));

            JsValue[] timeSeriesArray = new JsValue[timeSeries.Length];
            for (var i = 0; i < timeSeries.Length; i++)
                timeSeriesArray[i] = timeSeries[i]?.ToString();

            return new JsHandleJint(Engine.Realm.Intrinsics.Array.Construct(timeSeriesArray));

            static ArrayInstance EmptyArray(Engine engine)
            {
                return engine.Realm.Intrinsics.Array.Construct(0);
            }
        }

        public override JsHandleJint GetDocumentId(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length != 1 && args.Length != 2) //length == 2 takes into account Query Arguments that can be added to args
                throw new InvalidOperationException("id(doc) must be called with a single argument");

            if (args[0].IsNull || args[0].IsUndefined)
                return args[0];

            if (args[0].IsObject == false)
                throw new InvalidOperationException("id(doc) must be called with an object argument");

            var objectInstance = args[0].AsObject();

            if (objectInstance is BlittableObjectInstanceJint doc && doc.DocumentId != null)
            {
                var jsString = new JsString(doc.DocumentId);
                return new JsHandleJint(jsString);
            }

            var objectInstance2 = (ObjectInstance)objectInstance;
            var jsValue = objectInstance2.Get(Constants.Documents.Metadata.Key);
            // search either @metadata.@id or @id
            var metadata = jsValue.IsObject() == false ? objectInstance2 : jsValue.AsObject();
            var value = metadata.Get(Constants.Documents.Metadata.Id);
            if (value.IsString() == false)
            {
                // search either @metadata.Id or Id
                value = metadata.Get(Constants.Documents.Metadata.IdProperty);
                if (value.IsString() == false)
                    return new JsHandleJint(JsValue.Null);
            }
            return new JsHandleJint(value);
        }

        public override IBlittableObjectInstance CreateBlittableObjectInstanceFromScratch(IBlittableObjectInstance parent, BlittableJsonReaderObject blittable,
            string id, DateTime? lastModified, string changeVector)
        {
            return new BlittableObjectInstanceJint(Engine, (BlittableObjectInstanceJint)parent, blittable, id, lastModified,
                changeVector);
        }

        public override IBlittableObjectInstance CreateBlittableObjectInstanceFromDoc(IBlittableObjectInstance parent, BlittableJsonReaderObject blittable,
            Document doc)
        {
            return new BlittableObjectInstanceJint(Engine, (BlittableObjectInstanceJint)parent, blittable, doc);
        }

        public override IObjectInstance<JsHandleJint> CreateTimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment)
        {
            return new TimeSeriesSegmentObjectInstanceJint(EngineEx, segment);
        }

        public override IObjectInstance<JsHandleJint> CreateCounterEntryObjectInstance(DynamicCounterEntry entry)
        {
            return new CounterEntryObjectInstanceJint(EngineEx, entry);
        }

        public override JsHandleJint TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false)
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
                return new JsHandleJint(new BlittableObjectInstanceJint(Engine, null, context.ReadObject(djv, "MaterializedStreamResults"), null, null, null));
            }
            if (o is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?, ProjectionOptions> t)
            {
                var d = t.Item1;
                return new JsHandleJint(new BlittableObjectInstanceJint(Engine, null, Clone(d.Data, context), d)
                {
                    LuceneDocument = t.Item2,
                    LuceneState = t.Item3,
                    LuceneIndexFields = t.Item4,
                    LuceneAnyDynamicIndexFields = t.Item5 ?? false,
                    Projection = t.Item6
                });
            }
            if (o is Document doc)
            {
                return new JsHandleJint(new BlittableObjectInstanceJint(Engine, null, Clone(doc.Data, context), doc));
            }
            if (o is DocumentConflict dc)
            {
                return new JsHandleJint(new BlittableObjectInstanceJint(Engine, null, Clone(dc.Doc, context), dc.Id, dc.LastModified, dc.ChangeVector));
            }

            if (o is BlittableJsonReaderObject json)
            {
                return new JsHandleJint(new BlittableObjectInstanceJint(Engine, null, Clone(json, context), null, null, null));
            }

            if (o == null)
                return new JsHandleJint(Undefined.Instance);
            if (o is long lng)
                return new JsHandleJint(new JsBigInt(lng));
            if (o is BlittableJsonReaderArray bjra)
            {
                var jsArray = Engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
                var args = new JsHandleJint[1];
                for (var i = 0; i < bjra.Length; i++)
                {
                    args[0] = TranslateToJs(context, bjra[i]);
                    Engine.Realm.Intrinsics.Array.PrototypeObject.Push(jsArray, args.ToJsValueArray());
                }
                return new JsHandleJint(jsArray);
            }
            if (o is List<object> list)
            {
                var jsArray = Engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
                var args = new JsHandleJint[1];
                for (var i = 0; i < list.Count; i++)
                {
                    args[0] = TranslateToJs(context, list[i]);
                    Engine.Realm.Intrinsics.Array.PrototypeObject.Push(jsArray, args.ToJsValueArray());
                }
                return new JsHandleJint(jsArray);
            }
            if (o is List<Document> docList)
            {
                var jsArray = Engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
                var args = new JsValue[1];
                for (var i = 0; i < docList.Count; i++)
                {
                    args[0] = new BlittableObjectInstanceJint(Engine, null, Clone(docList[i].Data, context), docList[i]);
                    Engine.Realm.Intrinsics.Array.PrototypeObject.Push(jsArray, args);
                }
                return new JsHandleJint(jsArray);
            }
            // for admin
            if (o is RavenServer || o is DocumentDatabase)
            {
                AssertAdminScriptInstance();
                return new JsHandleJint(JsValue.FromObject(Engine, o));
            }
            //TODO: egor consider doing new JsNumber() inside JsHandleJint ctor
            if (o is bool b)
                return new JsHandleJint(b ? JsBoolean.True : JsBoolean.False);
            if (o is int integer)
                return new JsHandleJint(new JsNumber(integer));
            if (o is double dbl)
                return new JsHandleJint(new JsNumber(dbl));
            if (o is string s)
                return new JsHandleJint(new JsString(s));
            if (o is LazyStringValue ls)
                return new JsHandleJint(new JsString(ls.ToString()));
            if (o is LazyCompressedStringValue lcs)
                return new JsHandleJint(new JsString(lcs.ToString()));
            if (o is LazyNumberValue lnv)
            {
                return new JsHandleJint(BlittableObjectInstanceJint.BlittableObjectProperty.GetJsValueForLazyNumber(Engine, lnv));
            }
            if (o is JsValue js)
                return new JsHandleJint(js);
            throw new InvalidOperationException("No idea how to convert " + o + " to JsValue");
        }
    }
}
