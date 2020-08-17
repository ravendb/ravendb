using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.Documents.Indexes;
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

        public bool ReadOnly;

        public JavaScriptUtils(ScriptRunner runner, Engine engine)
        {
            _runner = runner;
            _scriptEngine = engine;
        }

        internal JsValue GetMetadata(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstance boi))
                throw new InvalidOperationException("getMetadata(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return JsValue.Null;
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

            metadata = Context.ReadObject(metadata, boi.DocumentId);
            JsValue metadataJs = TranslateToJs(_scriptEngine, Context, metadata);
            boi.Set(new JsString("@metadata"), metadataJs);
            return metadataJs;
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

        internal JsValue TranslateToJs(Engine engine, JsonOperationContext context, object o)
        {
            if (o is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?> t)
            {
                var d = t.Item1;
                return new BlittableObjectInstance(engine, null, Clone(d.Data, context), d)
                {
                    LuceneDocument = t.Item2,
                    LuceneState = t.Item3,
                    LuceneIndexFields = t.Item4,
                    LuceneAnyDynamicIndexFields = t.Item5 ?? false
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
                return new BlittableObjectInstance(engine, null, Clone(json, context), null, null, null);
            }

            if (o == null)
                return Undefined.Instance;
            if (o is long lng)
                return lng;
            if (o is BlittableJsonReaderArray bjra)
            {
                var jsArray = engine.Array.Construct(Array.Empty<JsValue>());
                var args = new JsValue[1];
                for (var i = 0; i < bjra.Length; i++)
                {
                    args[0] = TranslateToJs(engine, context, bjra[i]);
                    engine.Array.PrototypeObject.Push(jsArray, args);
                }
                return jsArray;
            }
            if (o is List<object> list)
            {
                var jsArray = engine.Array.Construct(Array.Empty<JsValue>());
                var args = new JsValue[1];
                for (var i = 0; i < list.Count; i++)
                {
                    args[0] = TranslateToJs(engine, context, list[i]);
                    engine.Array.PrototypeObject.Push(jsArray, args);
                }
                return jsArray;
            }
            if (o is List<Document> docList)
            {
                var jsArray = engine.Array.Construct(Array.Empty<JsValue>());
                var args = new JsValue[1];
                for (var i = 0; i < docList.Count; i++)
                {
                    args[0] = new BlittableObjectInstance(engine, null, Clone(docList[i].Data, context), docList[i]);
                    engine.Array.PrototypeObject.Push(jsArray, args);
                }
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
            _disposables.Clear();
            _context = null;
        }

        public void Reset(JsonOperationContext ctx)
        {
            _context = ctx;
        }
    }
}
