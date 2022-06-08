using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch;

public abstract class JavaScriptUtilsBase<T> : IJavaScriptUtils<T> where T : struct, IJsHandle<T>
{
    private IJsEngineHandle<T> _engine;

    public IJsEngineHandle<T> EngineHandle => _engine;

    public JsonOperationContext Context
    {
        get
        {
            Debug.Assert(_context != null, "_context != null");
            return _context;
        }
    }

    protected JsonOperationContext _context;
    protected readonly ScriptRunner<T> _runnerBase;
    protected readonly List<IDisposable> _disposables = new List<IDisposable>();

    private bool _readOnly;

    public bool ReadOnly
    {
        get { return _readOnly; }
        set { _readOnly = value; }
    }

    public abstract IBlittableObjectInstance<T> CreateBlittableObjectInstanceFromScratch(IBlittableObjectInstance<T> parent, BlittableJsonReaderObject blittable, string id, DateTime? lastModified, string changeVector);
    public abstract IBlittableObjectInstance<T> CreateBlittableObjectInstanceFromDoc(IBlittableObjectInstance<T> parent, BlittableJsonReaderObject blittable, Document doc);
    public abstract IObjectInstance<T> CreateTimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment);
    public abstract IObjectInstance<T> CreateCounterEntryObjectInstance(DynamicCounterEntry entry);
    public abstract T TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false);
    public abstract T GetMetadata(T self, T[] args);
    public abstract T AttachmentsFor(T self, T[] args);
    public abstract T LoadAttachment(T self, T[] args);
    public abstract T LoadAttachments(T self, T[] args);
    public abstract T GetTimeSeriesNamesFor(T self, T[] args);
    public abstract T GetCounterNamesFor(T self, T[] args);
    public abstract T GetDocumentId(T self, T[] args);

    public JavaScriptUtilsBase(ScriptRunner<T> runner, IJsEngineHandle<T> engine)
    {
        _runnerBase = runner;
        _engine = engine;
    }

    protected void AssertAdminScriptInstance()
    {
        if (_runnerBase._enableClr == false)
            throw new InvalidOperationException("Unable to run admin scripts using this instance of the script runner, the EnableClr is set to false");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected BlittableJsonReaderObject Clone(BlittableJsonReaderObject origin, JsonOperationContext context)
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
