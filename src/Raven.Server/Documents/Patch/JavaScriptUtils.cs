using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Global;
using Jint.Native.Object;
using Jint.Runtime;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class JavaScriptUtils
    {
        private JsonOperationContext Context
        {
            get
            {
                Debug.Assert(_context != null, "_context != null");
                return _context;
            }
        }

        private JsonOperationContext _context;
        private readonly ScriptRunner _runner;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        private readonly Engine _scriptEngine;
        internal JsValue CurrentlyProcessedObject;

        public bool ReadOnly;

        public JavaScriptUtils(ScriptRunner runner, Engine engine)
        {
            _runner = runner;
            _scriptEngine = engine;
        }

        internal JsValue Count(JsValue self, JsValue[] args)
        {
            if (args.Length != 0 || (CurrentlyProcessedObject is not BlittableObjectInstance doc)) 
            {
                throw new InvalidOperationException("count() must be called without arguments");
            }
            
            if (doc.Projection._query.Metadata.IsDynamic == false)
            {
                throw new InvalidOperationException("count() can only be used with dynamic index");
            }
            
            var getResult = doc.TryGetValue(Constants.Documents.Indexing.Fields.CountFieldName, out var countValue);
            
            Debug.Assert(getResult, $"Expected to get '{Constants.Documents.Indexing.Fields.CountFieldName}' field in map-reduce result: {doc.Blittable}");
            
            return countValue;
        }
        
        internal JsValue Key(JsValue self, JsValue[] args)
        {
            if (args.Length != 0 || (CurrentlyProcessedObject is not BlittableObjectInstance doc)) 
            {
                throw new InvalidOperationException("key() must be called without arguments");
            }

            if (doc.Projection._query.Metadata.IsDynamic == false)
            {
                throw new InvalidOperationException("key() can only be used with dynamic index");
            }
            
            var groupByFields = doc.Projection._query.Metadata.GroupBy;

            if (groupByFields.Length == 1)
            {
                var getResult = doc.TryGetValue(groupByFields[0].Name.Value, out var keyValue);
                
                Debug.Assert(getResult, $"Expected to get '{groupByFields[0].Name.Value}' field in map-reduce result: {doc.Blittable}");

                return keyValue;
            }

            else
            {
                var res = new DynamicJsonValue();

                foreach (var field in groupByFields)
                {
                    if (doc.Blittable.TryGetMember(field.Name.Value, out var keyValue))
                        res.Properties.Add((field.Name.Value, keyValue));
                }

                var jsonFromCtx = Context.ReadObject(res, null);
                JsValue resJs = TranslateToJs(_scriptEngine, Context, jsonFromCtx);

                return resJs;
            }
        }
        
        internal JsValue Sum(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || CurrentlyProcessedObject is not BlittableObjectInstance doc)
            {
                throw new InvalidOperationException("sum(doc => doc.fieldName) must be called with a single arrow function expression argument");
            }
            
            if (doc.Projection._query.Metadata.IsDynamic == false)
            {
                throw new InvalidOperationException("sum(doc => doc.fieldName) can only be used with dynamic index");
            }

            if (args[0] is ScriptFunctionInstance sfi)
            {
                if (sfi.FunctionDeclaration.ChildNodes[1] is StaticMemberExpression sme)
                {
                    if (sme.Property is Identifier identifier)
                    { 
                        var getResult = doc.TryGetValue(identifier.Name, out var sumValue);
                        
                        Debug.Assert(getResult, $"Expected to get '{identifier.Name}' field in map-reduce result: {doc.Blittable}");
                        
                        return sumValue;
                    }
                }
            }

            throw new InvalidOperationException("sum(doc => doc.fieldName) must be called with arrow function expression that points to field you want to aggregate");
        }        

        internal JsValue GetMetadata(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 && args.Length != 2 || //length == 2 takes into account Query Arguments that can be added to args
                !(args[0].AsObject() is BlittableObjectInstance boi)) 
                throw new InvalidOperationException("metadataFor(doc) must be called with a single entity argument");

            var modifiedMetadata = new DynamicJsonValue();

            // we need to set the metadata on the blittable itself, because we are might get the actual blittable here instead of Document
            if (string.IsNullOrEmpty(boi.ChangeVector) == false)
                modifiedMetadata[Constants.Documents.Metadata.ChangeVector] = boi.ChangeVector;
            if (string.IsNullOrEmpty(boi.DocumentId) == false)
                modifiedMetadata[Constants.Documents.Metadata.Id] = boi.DocumentId;
            if (boi.LastModified != null)
                modifiedMetadata[Constants.Documents.Metadata.LastModified] = boi.LastModified;

            if (boi.Blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.Modifications = modifiedMetadata;
            }

            if (boi.IndexScore != null)
                modifiedMetadata[Constants.Documents.Metadata.IndexScore] = boi.IndexScore.Value;

            if (boi.Distance != null)
                modifiedMetadata[Constants.Documents.Metadata.SpatialResult] = boi.Distance.Value.ToJson();

            // we cannot dispose the metadata here because the BOI is accessing blittable directly using the .Blittable property
            //using (var old = metadata)
            {
                metadata = metadata == null ? // may be null if we are working on map/redeuce index
                    Context.ReadObject(modifiedMetadata, boi.DocumentId) : 
                    Context.ReadObject(metadata, boi.DocumentId);
                
                JsValue metadataJs = TranslateToJs(_scriptEngine, Context, metadata);
                boi.Set(Constants.Documents.Metadata.Key, metadataJs);

                return metadataJs;
            }
        }

        internal JsValue AttachmentsFor(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstance boi))
                throw new InvalidOperationException($"{nameof(AttachmentsFor)} must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return EmptyArray(_scriptEngine);

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return EmptyArray(_scriptEngine);

            JsValue[] attachmentsArray = new JsValue[attachments.Length];
            for (var i = 0; i < attachments.Length; i++)
                attachmentsArray[i] = new AttachmentNameObjectInstance(_scriptEngine, (BlittableJsonReaderObject)attachments[i]);

            return _scriptEngine.Array.Construct(attachmentsArray);

            static ArrayInstance EmptyArray(Engine engine)
            {
                return engine.Array.Construct(0);
            }
        }

        internal JsValue LoadAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with two arguments, but '{args.Length}' were passed.");

            if (args[0].IsNull())
                return DynamicJsNull.ImplicitNull;

            if (args[0].IsObject() == false)
                ThrowInvalidFirstParameter();

            var doc = args[0].AsObject() as BlittableObjectInstance;
            if (doc == null)
                ThrowInvalidFirstParameter();

            if (args[1].IsString() == false)
                ThrowInvalidSecondParameter();

            var attachmentName = args[1].AsString();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            var attachment = CurrentIndexingScope.Current.LoadAttachment(doc.DocumentId, attachmentName);
            if (attachment is DynamicNullObject)
                return DynamicJsNull.ImplicitNull;

            return new AttachmentObjectInstance(_scriptEngine, (DynamicAttachment)attachment);

            void ThrowInvalidFirstParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            void ThrowInvalidSecondParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {args[1].GetType().FullName}.");
            }
        }

        internal JsValue LoadAttachments(JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with one argument, but '{args.Length}' were passed.");

            if (args[0].IsNull())
                return DynamicJsNull.ImplicitNull;

            if (args[0].IsObject() == false)
                ThrowInvalidParameter();

            var doc = args[0].AsObject() as BlittableObjectInstance;
            if (doc == null)
                ThrowInvalidParameter();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            var attachments = CurrentIndexingScope.Current.LoadAttachments(doc.DocumentId, GetAttachmentNames());
            if (attachments.Count == 0)
                return EmptyArray(_scriptEngine);

            var values = new JsValue[attachments.Count];
            for (var i = 0; i < values.Length; i++)
                values[i] = new AttachmentObjectInstance(_scriptEngine, attachments[i]);

            var array = _scriptEngine.Array.Construct(values.Length);
            _scriptEngine.Array.PrototypeObject.Push(array, values);

            return array;

            void ThrowInvalidParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            IEnumerable<string> GetAttachmentNames()
            {
                var metadata = doc.Get(Constants.Documents.Metadata.Key) as BlittableObjectInstance;
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
                return engine.Array.Construct(0);
            }
        }

        internal JsValue GetTimeSeriesNamesFor(JsValue self, JsValue[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.TimeSeries, "timeSeriesNamesFor");
        }

        internal JsValue GetCounterNamesFor(JsValue self, JsValue[] args)
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.Counters, "counterNamesFor");
        }

        private JsValue GetNamesFor(JsValue self, JsValue[] args, string metadataKey, string methodName)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstance boi))
                throw new InvalidOperationException($"{methodName}(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return EmptyArray(_scriptEngine);

            if (metadata.TryGet(metadataKey, out BlittableJsonReaderArray timeSeries) == false)
                return EmptyArray(_scriptEngine);

            JsValue[] timeSeriesArray = new JsValue[timeSeries.Length];
            for (var i = 0; i < timeSeries.Length; i++)
                timeSeriesArray[i] = timeSeries[i]?.ToString();

            return _scriptEngine.Array.Construct(timeSeriesArray);

            static ArrayInstance EmptyArray(Engine engine)
            {
                return engine.Array.Construct(0);
            }
        }

        internal JsValue GetDocumentId(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 && args.Length != 2) //length == 2 takes into account Query Arguments that can be added to args
                throw new InvalidOperationException("id(doc) must be called with a single argument");

            if (args[0].IsNull() || args[0].IsUndefined())
                return args[0];

            if (args[0].IsObject() == false)
                throw new InvalidOperationException("id(doc) must be called with an object argument");

            var objectInstance = args[0].AsObject();

            if (objectInstance is BlittableObjectInstance doc && doc.DocumentId != null)
                return doc.DocumentId;

            var jsValue = objectInstance.Get(Constants.Documents.Metadata.Key);
            // search either @metadata.@id or @id
            var metadata = jsValue.IsObject() == false ? objectInstance : jsValue.AsObject();
            var value = metadata.Get(Constants.Documents.Metadata.Id);
            if (value.IsString() == false)
            {
                // search either @metadata.Id or Id
                value = metadata.Get(Constants.Documents.Metadata.IdProperty);
                if (value.IsString() == false)
                    return JsValue.Null;
            }
            return value;
        }

        internal JsValue TranslateToJs(Engine engine, JsonOperationContext context, object o, bool needsClone = true)
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
                return new BlittableObjectInstance(engine, null, context.ReadObject(djv, "MaterializedStreamResults"), null, null, null);
            }
            if (o is Tuple<Document, RetrieverInput, Dictionary<string, IndexField>, bool?, ProjectionOptions> t)
            {
                var d = t.Item1;
                return new BlittableObjectInstance(engine, null, Clone(d.Data, context), d)
                {
                    IndexRetriever = t.Item2,
                    IndexFields = t.Item3,
                    AnyDynamicIndexFields = t.Item4 ?? false,
                    Projection = t.Item5
                };
            }
            if (o is Document doc)
            {
                return new BlittableObjectInstance(engine, null, Clone(doc.Data, context), doc);
            }
            if (o is DocumentConflict dc)
            {
                return new BlittableObjectInstance(engine, null, Clone(dc.Doc, context), dc.Id, dc.LastModified, dc.ChangeVector);
            }

            if (o is BlittableJsonReaderObject json)
            {
                // check if clone is really required, we don't want to clone patch arguments
                BlittableJsonReaderObject blittable = needsClone ? Clone(json, context) : json;
                return new BlittableObjectInstance(engine, null, blittable, null, null, null);
            }

            if (o == null)
                return Undefined.Instance;
            if (o is long lng)
                return lng;
            if (o is BlittableJsonReaderArray bjra)
            {
                var jsArray = engine.Array.Construct(bjra.Length);
                var args = new JsValue[bjra.Length];
                for (var i = 0; i < bjra.Length; i++)
                {
                    args[i] = TranslateToJs(engine, context, bjra[i]);
                }
                engine.Array.PrototypeObject.Push(jsArray, args);
                return jsArray;
            }
            if (o is List<object> list)
            {
                var jsArray = engine.Array.Construct(list.Count);
                var args = new JsValue[list.Count];
                for (var i = 0; i < list.Count; i++)
                {
                    args[i] = TranslateToJs(engine, context, list[i]);
                }
                engine.Array.PrototypeObject.Push(jsArray, args);
                return jsArray;
            }
            if (o is List<Document> docList)
            {
                var jsArray = engine.Array.Construct(docList.Count);
                var args = new JsValue[docList.Count];
                for (var i = 0; i < docList.Count; i++)
                {
                    args[i] = new BlittableObjectInstance(engine, null, Clone(docList[i].Data, context), docList[i]);
                }
                engine.Array.PrototypeObject.Push(jsArray, args);
                return jsArray;
            }
            // for admin
            if (o is RavenServer || o is DocumentDatabase)
            {
                AssertAdminScriptInstance();
                return JsValue.FromObject(engine, o);
            }
            if (o is ObjectInstance j)
                return j;
            if (o is bool b)
                return b ? JsBoolean.True : JsBoolean.False;
            if (o is int integer)
                return integer;
            if (o is double dbl)
                return dbl;
            if (o is string s)
                return s;
            if (o is LazyStringValue ls)
                return ls.ToString();
            if (o is LazyCompressedStringValue lcs)
                return lcs.ToString();
            if (o is LazyNumberValue lnv)
            {
                return BlittableObjectInstance.BlittableObjectProperty.GetJsValueForLazyNumber(engine, lnv);
            }
            if (o is JsValue js)
                return js;
            throw new InvalidOperationException("No idea how to convert " + o + " to JsValue");
        }

        private void AssertAdminScriptInstance()
        {
            if (_runner._enableClr == false)
                throw new InvalidOperationException("Unable to run admin scripts using this instance of the script runner, the EnableClr is set to false");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private BlittableJsonReaderObject Clone(BlittableJsonReaderObject origin, JsonOperationContext context)
        {
            if (ReadOnly)
                return origin;

            var noCache = origin.NoCache;
            origin.NoCache = true;
            // RavenDB-8286
            // here we need to make sure that we aren't sending a value to
            // the js engine that might be modified by the actions of the js engine
            // for example, calling put() might cause the original data to change
            // because we defrag the data that we looked at. We are handling this by
            // ensuring that we have our own, safe, copy.
            var cloned = origin.Clone(context);
            cloned.NoCache = true;
            _disposables.Add(cloned);

            origin.NoCache = noCache;
            return cloned;
        }

        public void Clear()
        {
            foreach (var disposable in _disposables)
                disposable.Dispose();
            CurrentlyProcessedObject = null;
            _disposables.Clear();
            _context = null;
        }

        public void Reset(JsonOperationContext ctx)
        {
            _context = ctx;
        }
    }
}
