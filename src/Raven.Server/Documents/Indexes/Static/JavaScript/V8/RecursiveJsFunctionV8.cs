using System;
using System.Collections.Generic;
using Raven.Server.Documents.Patch;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public class RecursiveJsFunctionV8 : IDisposable
    {
        private bool _disposed = false;

        private InternalHandle _result;
        private readonly V8Engine _engine;
        private readonly InternalHandle _item;
        private readonly InternalHandle _func;
        private readonly HashSet<InternalHandle> _results = new HashSet<InternalHandle>();
        private readonly Queue<NullIfEmptyEnumerableResult> _queue = new Queue<NullIfEmptyEnumerableResult>();

        public RecursiveJsFunctionV8(V8Engine engine, InternalHandle item, InternalHandle func)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _item = item;

            if (func.IsUndefined || func.IsNull)
                throw new ArgumentNullException(nameof(func));
            _func = func;
        }

        ~RecursiveJsFunctionV8()
        {
            Dispose(false);
        }

        public void Dispose()
        {  
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                GC.SuppressFinalize(this);
            }

            foreach (var res in _results)
            {
                res.Dispose();
            }
            _results.Clear();

            _disposed = true;
        }


        public InternalHandle Execute()
        {
            _result.Set(_engine.CreateArray(Array.Empty<InternalHandle>()));

            if (_item.IsUndefined)
                return _result;

            using (var jsRes = _func.StaticCall(_item))
            {
                jsRes.ThrowOnError(); 
                var current = NullIfEmptyEnumerable(jsRes);
                if (current.Kind != NullIfEmptyEnumerableKind.None)
                {
                    using (var jsResPush = _result.StaticCall("push", _item))
                        jsResPush.ThrowOnError(); 
                    return _result;
                }
                
                _queue.Enqueue(new NullIfEmptyEnumerableResult
                {
                    Kind = NullIfEmptyEnumerableKind.Handle,
                    Handle = _item
                });
                while (_queue.Count > 0)
                {
                    current = _queue.Dequeue();

                    if (current.Kind == NullIfEmptyEnumerableKind.Enumerable)
                    {
                        var list = current.Enumerable;
                        foreach (InternalHandle o in list)
                            AddItem(o);
                    }
                    if (current.Kind == NullIfEmptyEnumerableKind.Handle)
                        AddItem(current.Handle);
                }
            }

            return _result;
        }

        private void AddItem(InternalHandle current)
        {
            if (_results.Add(current) == false)
                return;

            using (var jsResPush = _result.StaticCall("push", current))
                jsResPush.ThrowOnError();

            using (var jsRes = _func.StaticCall(current))
            {
                jsRes.ThrowOnError();
                var result = NullIfEmptyEnumerable(jsRes);
                if (result.Kind != NullIfEmptyEnumerableKind.None)
                    _queue.Enqueue(result);
            }
        }

        private enum NullIfEmptyEnumerableKind
        {
            None = 0,
            Handle = 1,
            Enumerable = 2
            
        }
        
        private struct NullIfEmptyEnumerableResult
        {
            public NullIfEmptyEnumerableKind Kind;
            public InternalHandle Handle;
            public IEnumerable<InternalHandle> Enumerable;
        }

        private static NullIfEmptyEnumerableResult NullIfEmptyEnumerable(InternalHandle item)
        {
            var result = new NullIfEmptyEnumerableResult
            {
                Kind = NullIfEmptyEnumerableKind.None
            };
            if (item.IsArray == false)
            {
                result.Kind = NullIfEmptyEnumerableKind.Handle;
                result.Handle = item;
            }
            else if (item.ArrayLength == 0)
            {
                result.Kind = NullIfEmptyEnumerableKind.None;
            }
            else
            {
                result.Kind = NullIfEmptyEnumerableKind.Enumerable;
                result.Enumerable = Yield(item);
            }

            return result;
        }

        private static IEnumerable<InternalHandle> Yield(InternalHandle jsArray)
        {
            int arrayLength =  jsArray.ArrayLength;
            for (int i = 0; i < arrayLength; ++i)
                yield return jsArray.GetProperty(i);
        }
    }
}
