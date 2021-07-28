using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using V8.Net;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;
using Sparrow.Json.Parsing;

//using Raven.Server.Documents.Indexes.Static.Counters;
//using Raven.Server.Documents.Indexes.Static.TimeSeries;

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
        private readonly V8Engine Engine;

        public bool ReadOnly;

        public JavaScriptUtils(ScriptRunner runner, V8Engine engine)
        {
            _runner = runner;
            Engine = engine;
        }

        internal InternalHandle GetMetadata(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1 || !(args[0].BoundObject is BlittableObjectInstance boi))
                throw new InvalidOperationException("metadataFor(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return Engine.CreateNullValue();
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

            using (var old = metadata)
            {
                metadata = Context.ReadObject(metadata, boi.DocumentId);
                InternalHandle metadataJs = TranslateToJs(Context, metadata);
                args[0].SetProperty(Constants.Documents.Metadata.Key, metadataJs);

                return metadataJs;
            }
        }

        internal InternalHandle AttachmentsFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1 || !(args[0].BoundObject is BlittableObjectInstance boi))
                throw new InvalidOperationException($"{nameof(AttachmentsFor)} must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return EmptyArray(Engine);

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return EmptyArray(Engine);

            int arrayLength =  attachments.Count;
            InternalHandle[] jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; i++)
                jsItems[i] = Engine.CreateObjectInstance(new AttachmentNameObjectInstance((BlittableJsonReaderObject)attachments[i]), TypeBinderAttachmentNameObjectInstance);

            return Engine.CreateArrayWithDisposal(jsItems);

            static InternalHandle EmptyArray(V8Engine engine)
            {
                return engine.CreateArray(Array.Empty<InternalHandle>());
            }
        }

        internal InternalHandle LoadAttachment(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 2)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with two arguments, but '{args.Length}' were passed.");

            InternalHandle jsRes;
            if (args[0].IsNull)
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            if (args[0].IsObject == false)
                ThrowInvalidFirstParameter();

            var doc = args[0].BoundObject as BlittableObjectInstance;
            if (doc == null)
                ThrowInvalidFirstParameter();

            if (args[1].IsString == false)
                ThrowInvalidSecondParameter();

            var attachmentName = args[1].AsString;

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            var attachment = CurrentIndexingScope.Current.LoadAttachment(doc.DocumentId, attachmentName);
            if (attachment.BoundObject is DynamicNullObject)
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            return new AttachmentObjectInstance(engine, (DynamicAttachment)attachment);

            void ThrowInvalidFirstParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            void ThrowInvalidSecondParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {args[1].GetType().FullName}.");
            }
        }

        internal InternalHandle LoadAttachments(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with one argument, but '{args.Length}' were passed.");

            InternalHandle jsRes;
            if (args[0].IsNull())
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            if (args[0].IsObject == false)
                ThrowInvalidParameter();

            var doc = args[0].BoundObject as BlittableObjectInstance;
            if (doc == null)
                ThrowInvalidParameter();

            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            var attachments = CurrentIndexingScope.Current.LoadAttachments(doc.DocumentId, GetAttachmentNames());
            if (attachments.Count == 0)
                return Engine.CreateArray(Array.Empty<InternalHandle>());

            int arrayLength =  attachments.Count;
            var jsItems = new InternalHandle[attachments.Count];
            for (int i = 0; i < arrayLength; i++)
                jsItems.Set(engine.CreateObjectBinder(new AttachmentObjectInstance(attachments[i]), TypeBinderAttachmentObjectInstance)._);

            return this.CreateArrayWithDisposal(jsItems);


            void ThrowInvalidParameter()
            {
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {args[0].GetType().FullName}.");
            }

            IEnumerable<string> GetAttachmentNames()
            {
                using (var jsMetadata = doc.Call(ScriptRunner.SingleRun.GetMetadataMethod, InternalHandle.Empty))
                {
                    jsMetadata.ThrowOnError(); // TODO check if is needed here
                    if (jsMetadata.IsUndefined || jsMetadata.IsNull)
                        yield break;

                    using (var jsAttachments = jsMetadata.GetProperty(Constants.Documents.Metadata.Attachments))
                    {
                        if (jsAttachments.IsUndefined || jsAttachments.IsNull || jsAttachments.IsArray == false)
                            yield break;

                        int arrayLength =  jsAttachments.ArrayLength;
                        for (int i = 0; i < arrayLength; ++i)
                        {
                            using (var jsAttachment = jsAttachments.GetProperty(i))
                            {
                                using (var jsAttachmentName = jsAttachment.GetProperty(nameof(AttachmentName.Name)))
                                    yield return jsAttachmentName.AsString;
                            }
                        }
                    }
                }
            }
        }

        internal InternalHandle GetTimeSeriesNamesFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.TimeSeries, "timeSeriesNamesFor");
        }

        internal InternalHandle GetCounterNamesFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            return GetNamesFor(self, args, Constants.Documents.Metadata.Counters, "counterNamesFor");
        }

        private InternalHandle GetNamesFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, InternalHandle[] args, string metadataKey, string methodName)
        {
            if (args.Length != 1 || !(args[0].BoundObject is BlittableObjectInstance boi))
                throw new InvalidOperationException($"{methodName}(doc) must be called with a single entity argument");

            if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                return Engine.CreateArray(Array.Empty<InternalHandle>());

            if (metadata.TryGet(metadataKey, out BlittableJsonReaderArray timeSeries) == false)
                return Engine.CreateArray(Array.Empty<InternalHandle>());

            InternalHandle[] jsItems = new InternalHandle[timeSeries.Length];
            for (var i = 0; i < timeSeries.Length; i++)
                jsItems[i] = Engine.CreateValue(timeSeries[i]?.ToString());

            return Engine.CreateArrayWithDisposal(jsItems);
        }

        internal InternalHandle GetDocumentId(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1 && args.Length != 2) //length == 2 takes into account Query Arguments that can be added to args
                throw new InvalidOperationException("id(doc) must be called with a single argument");

            if (args[0].IsNull || args[0].IsUndefined)
                return args[0];

            if (args[0].IsObject == false)
                throw new InvalidOperationException("id(doc) must be called with an object argument");

            V8NativeOject objectInstance = args[0].Object;

            if (objectInstance is BlittableObjectInstance doc && doc.DocumentId != null)
                return doc.DocumentId;

            using (var jsValue = objectInstance.GetProperty(Constants.Documents.Metadata.Key))
            {
                // search either @metadata.@id or @id
                using (var metadata = jsValue.IsObject == false ? objectInstance : jsValue)
                {
                    var value = metadata.GetProperty(Constants.Documents.Metadata.Id);
                    if (value.IsString == false)
                    {
                        // search either @metadata.Id or Id
                        value.Dispose();
                        value = metadata.GetProperty(Constants.Documents.Metadata.IdProperty);
                        if (value.IsString == false)
                            value.Dispose();
                            return Engine.CreateNullValue();
                    }
                    return value;
                }
            }
        }

        internal InternalHandle TranslateToJs(JsonOperationContext context, object o)
        {
            InternalHandle jsRes;
            if (o is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?, ProjectionOptions> t)
            {
                var d = t.Item1;
                BlittableObjectInstance boi = new BlittableObjectInstance(Engine, null, Clone(d.Data, context), d)
                {
                    LuceneDocument = t.Item2,
                    LuceneState = t.Item3,
                    LuceneIndexFields = t.Item4,
                    LuceneAnyDynamicIndexFields = t.Item5 ?? false,
                    Projection = t.Item6
                };
                return jsRes.Set(boi.CreateObjectBinder()._);
            }
            if (o is Document doc)
            {
                BlittableObjectInstance boi = new BlittableObjectInstance(Engine, null, Clone(doc.Data, context), doc);
                return jsRes.Set(boi.CreateObjectBinder()._);
            }
            if (o is DocumentConflict dc)
            {
                BlittableObjectInstance boi = new BlittableObjectInstance(Engine, null, Clone(dc.Doc, context), dc.Id, dc.LastModified, dc.ChangeVector);
                return jsRes.Set(boi.CreateObjectBinder()._);
            }

            if (o is BlittableJsonReaderObject json)
            {
                BlittableObjectInstance boi = new BlittableObjectInstance(Engine, null, Clone(json, context), null, null, null);
                return jsRes.Set(boi.CreateObjectBinder()._);
            }

            if (o == null)
                return Undefined.Instance;
            if (o is long lng)
                return lng;
            if (o is BlittableJsonReaderArray bjra)
            {
                int arrayLength =  bjra.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, bjra[i]);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            if (o is List<object> list)
            {
                int arrayLength =  list.Count;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = TranslateToJs(context, list[i]);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            if (o is List<Document> docList)
            {
                int arrayLength =  docList.Count;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    BlittableObjectInstance boi = new BlittableObjectInstance(Engine, null, Clone(docList[i].Data, context), docList[i]);
                    jsItems[i].Set(boi.CreateObjectBinder()._);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            // for admin
            if (o is RavenServer || o is DocumentDatabase)
            {
                AssertAdminScriptInstance();
                return Engine.FromObject(o);
            }
            if (o is V8NativeObject j) {
                InternalHandle jsRes;
                return jsRes.Set(j._);
            }
            if (o is bool b)
                return Engine.CreateValue(b ? JsBoolean.True : JsBoolean.False);
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
                return BlittableObjectInstance.BlittableObjectProperty.GetJsValueForLazyNumber(Engine, lnv);
            }
            if (o is InternalHandle js)
                return js;
            throw new InvalidOperationException("No idea how to convert " + o + " to InternalHandle");
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

    public class V8EngineEx : V8Engine
    {
        public IDisposable ChangeMaxStatements(int value)
        {
            void DoNothing()
            {}

            return new DisposableAction(DoNothing);
        }

        public IDisposable DisableMaxStatements()
        {
            return ChangeMaxStatements(int.MaxValue);
        }


        public static void Dispose(InternalHandle[] jsItems)
        {
            for (int i = 0; i < jsItems.Length; ++i)
            {
                jsItems[i].Dispose();
            }
        }

        public InternalHandle CreateArrayWithDisposal(InternalHandle[] jsItems)
        {
            var jsArr = this.CreateArray(jsItems);
            V8EngineEx.Dispose(jsItems);
            return jsArr;
        }

        public readonly TypeBinder TypeBinderBlittableObjectInstance;
        public readonly TypeBinder TypeBinderTask;
        public readonly TypeBinder TypeBinderTimeSeriesSegmentObjectInstance;
        public readonly TypeBinder TypeBinderDynamicTimeSeriesEntries;
        public readonly TypeBinder TypeBinderDynamicTimeSeriesEntry;
        public readonly TypeBinder TypeBinderCounterEntryObjectInstance;
        public readonly TypeBinder TypeBinderAttachmentNameObjectInstance;
        public readonly TypeBinder TypeBinderAttachmentObjectInstance;

        V8EngineEx(bool autoCreateGlobalContext = true) : base(autoCreateGlobalContext)
        {
            this.ExecuteWithReset(ArrayExtensionCode, "arrayExtension");

            TypeBinderBlittableObjectInstance = this.RegisterType<BlittableObjectInstance>(null, true);
            TypeBinderBlittableObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<BlittableObjectInstance.CustomBinder, BlittableObjectInstance>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(BlittableObjectInstance));

            TypeBinderTask = this.RegisterType<Task>(null, true, ScriptMemberSecurity.ReadWrite);
            TypeBinderTask.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<TaskCustomBinder, Task>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(Task));


            TypeBinderTimeSeriesSegmentObjectInstance = this.RegisterType<TimeSeriesSegmentObjectInstance>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderTimeSeriesSegmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<TimeSeriesSegmentObjectInstance.CustomBinder, TimeSeriesSegmentObjectInstance>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(TimeSeriesSegmentObjectInstance));

            TypeBinderDynamicTimeSeriesEntries = this.RegisterType<DynamicArray>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderDynamicTimeSeriesEntries.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<DynamicTimeSeriesEntriesCustomBinder, DynamicArray>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(DynamicArray));

            TypeBinderDynamicTimeSeriesEntry = this.RegisterType<DynamicTimeSeriesEntry>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderDynamicTimeSeriesEntry.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<DynamicTimeSeriesEntryCustomBinder, DynamicTimeSeriesEntry>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(DynamicTimeSeriesEntry));


            TypeBinderCounterEntryObjectInstance = this.RegisterType<CounterEntryObjectInstance>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderCounterEntryObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<CounterEntryObjectInstance.CustomBinder, CounterEntryObjectInstance>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(CounterEntryObjectInstance));

            TypeBinderAttachmentNameObjectInstance = this.RegisterType<AttachmentNameObjectInstance>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderAttachmentNameObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<AttachmentNameObjectInstance.CustomBinder, AttachmentNameObjectInstance>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(AttachmentNameObjectInstance));

            TypeBinderAttachmentObjectInstance = this.RegisterType<AttachmentObjectInstance>(null, false, ScriptMemberSecurity.NoAccess);
            TypeBinderAttachmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<AttachmentObjectInstance.CustomBinder, AttachmentObjectInstance>(obj, initializeBinder);
            this.GlobalObject.SetProperty(typeof(AttachmentObjectInstance));


            var typeDynamicJsNull = this.RegisterType<DynamicJsNull>(null, true, ScriptMemberSecurity.NoAccess);
            this.GlobalObject.SetProperty(typeof(DynamicJsNull));

            var typeBinderObject = this.RegisterType<object>(null, true, ScriptMemberSecurity.ReadWrite);
            this.GlobalObject.SetProperty(typeof(object));

            //this.GlobalObject.SetProperty(typeof(Hash<Handle>));
        }

        private static string ArrayExtensionCode=  @"
            Array.prototype.orig = {}

            Array.prototype.orig.reduce = Array.prototype.reduce;
            Array.prototype.reduce = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.reduce(...args) :
                        args.length > 1 ? args[1] : null
            }

            Array.prototype.orig.concat = Array.prototype.concat;
            Array.prototype.concat = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.concat(...args) :
                    args.length > 0 ? args[0] : []
            }

            Array.prototype.orig.some = Array.prototype.some;
            Array.prototype.some = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.some(...args) : false
            }

            Array.prototype.orig.includes = Array.prototype.includes;
            Array.prototype.includes = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.includes(...args) : false
            }

            Array.prototype.orig.every = Array.prototype.every;
            Array.prototype.every = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.every(...args) : true
            }

            Array.prototype.orig.map = Array.prototype.map;
            Array.prototype.map = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.map(...args) : []
            }

            Array.prototype.orig.filter = Array.prototype.filter;
            Array.prototype.filter = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.filter(...args) : []
            }

            Array.prototype.orig.reverse = Array.prototype.reverse;
            Array.prototype.reverse = function(...args) {
                const v = this
                return v.length > 0 ? v.orig.reverse(...args) : []
            }
        ";
        
        internal Dictionary<Type, Func<V8Engine, object, InternalHandle>> TypeMappers = new Dictionary<Type, Func<V8Engine, object, InternalHandle>>()
        {
            {typeof(bool), (v) => this.CreateValue((bool) v)},
            {typeof(byte), (v) => this.CreateValue((byte) v)},
            {typeof(char), (v) => JsString.Create((char) v)},
            {typeof(TimeSpan), (v) => this.CreateValue((TimeSpan) v)},
            {typeof(DateTime), (v) => this.CreateValue((DateTime) v)},
            //{typeof(DateTimeOffset), (v) => engine.Realm.Intrinsics.Date.Construct((DateTimeOffset) v)},
            {typeof(decimal), (v) => this.CreateValue((double) (decimal) v)},
            {typeof(double), (v) => this.CreateValue((double) v)},
            {typeof(SByte), (v) => this.CreateValue((Int32) (SByte) v)},
            {typeof(Int16), (v) => this.CreateValue((Int32) (Int16) v)}, 
            {typeof(Int32), (v) => this.CreateValue((Int32) v)},
            {typeof(Int64), (v) => this.CreateIntValue((Int64) v)},
            {typeof(Single), (v) => this.CreateValue((double) (Single) v)},
            {typeof(string), (v) => this.CreateValue((string) v)},
            {typeof(UInt16), (v) => this.CreateUIntValue((UInt16) v)}, 
            {typeof(UInt32), (v) => this.CreateUIntValue((UInt32) v)},
            {typeof(UInt64), (v) => this.CreateUIntValue((UInt64) v)},
            {
                typeof(System.Text.RegularExpressions.Regex),
                (v) => engine.Realm.Intrinsics.RegExp.Construct((System.Text.RegularExpressions.Regex) v, "")
            }
        };

        internal static InternalHandle CreateUIntValue(uint value)
        {
            return value < int.MaxValue ? V8Engine.CreateValue((Int32) v) : V8Engine.CreateValue((double) v);
        }

        internal static InternalHandle CreateUIntValue(ulong value)
        {
            return value < int.MaxValue ? V8Engine.CreateValue((Int32) v) : V8Engine.CreateValue((double) v);
        }

        internal static InternalHandle CreateIntValue(long value)
        {
            return value < int.MaxValue && value > int.MinValue ? V8Engine.CreateValue((Int32) v) : V8Engine.CreateValue((double) v);
        }

        public ObjectBinder CreateObjectBinder(object obj, TypeBinder tb = null)
        {
            if (obj == null) {
                return null;
            }
            if (tb == null) {
                var type = object.GetType();
                tb = GetTypeBinder(type);
            }
            return TypeBinder.CreateObjectBinder<ObjectBinder<type>, type>(obj);
        }

        public InternalHandle FromObject(object value)
        {
            if (value == null)
            {
                return InternalHandle.Empty;
            }

            if (value is InternalHandle jsValue)
            {
                return jsValue;
            }

            Type t = value.GetType();
            if (t.IsEnum)
            {
                return this.CreateValue(value.ToString());
                
                // is overloaded with upper code
                /*Type ut = Enum.GetUnderlyingType(t);

                if (ut == typeof(ulong))
                    return this.CreateValue(System.Convert.ToDouble(value));

                if (ut == typeof(uint) || ut == typeof(long))
                    return this.CreateValue(System.Convert.ToInt64(value));

                return this.CreateValue(System.Convert.ToInt32(value));*/
            }

            jsValue = value switch 
            {
                //BlittableJsonReaderObject bjro => (new BlittableObjectInstance(this, null, bjro, null, null, null)).CreateObjectBinder(),
                //Document doc => (new BlittableObjectInstance(this, null, doc.Data, doc)).CreateObjectBinder(),
                //LazyNumberValue lnv => this.CreateValue(lnv.ToDouble(CultureInfo.InvariantCulture)),
                StringSegment ss => this.CreateValue(ss.ToString()),
                LazyStringValue lsv => this.CreateValue(lsv.ToString()),
                LazyCompressedStringValue lcsv => this.CreateValue(lcsv.ToString()),
                Guid guid => this.CreateValue(guid.ToString()),
                TimeSpan timeSpan => this.CreateValue(timeSpan.ToString()),
                DateTime dateTime => this.CreateValue(dateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)),
                DateTimeOffset dateTimeOffset => this.CreateValue(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)),
                _ => InternalHandle.Empty
            };

            if (jsValue.IsEmpty() == false) {
                return jsValue;
            }

            var valueType = value.GetType();

            var typeMappers = this.TypeMappers;

            if (typeMappers.TryGetValue(valueType, out var typeMapper))
            {
                return typeMapper(value);
            }

            /*var type = value as Type;
            if (type != null)
            {
                var typeReference = TypeReference.CreateTypeReference(this, type);
                return typeReference;
            }*/

            if (value is System.Array a)
            {
                // racy, we don't care, worst case we'll catch up later
                Interlocked.CompareExchange(ref this.TypeMappers, new Dictionary<Type, Func<V8Engine, object, InternalHandle>>(typeMappers)
                {
                    [valueType] = Convert
                }, typeMappers);

                return Convert(a);
            }

            if (value is Delegate d)
            {
                return new DelegateWrapper(this, d);
            }

            // if no known type could be guessed, wrap it as an ObjectBinder<object>
            InternalHandle jsRes;
            return jsRes.Set(CreateObjectBinder(value)._);
        }

        private InternalHandle Convert(object v)
        {
            var array = (System.Array) v;

            int arrayLength =  array.Length;
            var jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsItems[i] = Engine.FromObject(array.GetValue(i));
            }

            return this.CreateArrayWithDisposal(jsItems);
        }


        public static double ToNumber(InternalHandle o)
        {
            return o.AsDouble;
        }

    }

    public class ClrFunctionInstance : V8Function
    {
        public ClrFunctionInstance(
            V8Engine Engine,
            string name,
            Func<V8EngineEx, bool, InternalHandle, InternalHandle[], InternalHandle> func) : base()
        {
            _func = func;
        }
        public override InternalHandle Initialize(bool isConstructCall, params InternalHandle[] args)
        {
            Callback = CallbackMethod;

            return base.Initialize(isConstructCall, args);
        }

        public InternalHandle CallbackMethod(V8Engine engine, bool isConstructCall, InternalHandle _this, params InternalHandle[] args)
        {
            return _func((V8EngineEx)engine, isConstructCall, _this, args);
        }
    }

}
