using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public class RecursiveJsFunction
    {
        private InternalHandle _result;
        private readonly V8Engine _engine;
        private readonly InternalHandle _item;
        private readonly V8Function _func;
        private readonly HashSet<Handle> _results = new HashSet<Handle>();
        private readonly Queue<object> _queue = new Queue<object>();

        public RecursiveJsFunction(V8Engine engine, InternalHandle item, V8Function func)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _item.Set(item);
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        ~RecursiveJsFunction()
        {
            _item.Dispose();
            _result.Dispose();
        }

        public InternalHandle Execute()
        {
            _result.Set(_engine.CreateArray(Array.Empty<InternalHandle>()));

            if (_item.IsUndefined)
                return _result;

            using (var jsRes = _func.Call(InternalHandle.Empty, _item))
            {
                jsRes.ThrowOnError(); // TODO check if is needed here
                var current = NullIfEmptyEnumerable(jsRes);
                if (current == null)
                {
                    using (var jsResPush = _result.Call("push", InternalHandle.Empty, _item))
                        jsResPush.ThrowOnError(); // TODO check if is needed here
                    return _result;
                }

                _queue.Enqueue(_item);
                while (_queue.Count > 0)
                {
                    current = _queue.Dequeue();

                    var list = current as IEnumerable<InternalHandle>;
                    if (list != null)
                    {
                        foreach (InternalHandle o in list)
                            using (o)
                                AddItem(o);
                    }
                    else if (current is InternalHandle currentJs)
                        using (currentJs)
                            AddItem(currentJs);
                }
            }

            return _result;
        }

        private void AddItem(InternalHandle current)
        {
            if (_results.Add((Handle)current) == false)
                return;

            using (var jsResPush = _result.Call("push", InternalHandle.Empty, current))
                jsResPush.ThrowOnError(); // TODO check if is needed here

            using (var jsRes = _func.StaticCall(current))
            {
                jsRes.ThrowOnError(); // TODO check if is needed here
                var result = NullIfEmptyEnumerable(jsRes);
                if (result != null)
                    _queue.Enqueue(result);
            }
        }

        private static object NullIfEmptyEnumerable(InternalHandle item)
        {
            if (item.IsArray == false) {
                return new InternalHandle(item, true);
            }

            //using (item)
            {
                if (item.ArrayLength == 0)
                    return null;

                return Yield(item);
            }
        }

        private static IEnumerable<InternalHandle> Yield(InternalHandle jsArray)
        {
            int arrayLength =  jsArray.ArrayLength;
            for (int i = 0; i < arrayLength; ++i)
                yield return jsArray.GetProperty(i);
        }
    }
}
