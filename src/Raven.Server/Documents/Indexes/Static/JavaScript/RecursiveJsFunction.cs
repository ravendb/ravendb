using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public class RecursiveJsFunction
    {
        private readonly List<JsValue> _result = new List<JsValue>();
        private readonly Engine _engine;
        private readonly JsValue _item;
        private readonly ScriptFunctionInstance _func;
        private readonly HashSet<JsValue> _results = new HashSet<JsValue>();
        private readonly Queue<object> _queue = new Queue<object>();

        public RecursiveJsFunction(Engine engine, JsValue item, ScriptFunctionInstance func)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _item = item;
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public JsValue Execute()
        {
            if (_item == null)
                return _engine.Array.Construct(0);

            var current = NullIfEmptyEnumerable(_func.Invoke(_item));
            if (current == null)
            {
                _result.Add(_item);
                return _engine.Array.Construct(_result.ToArray());
            }

            _queue.Enqueue(_item);
            while (_queue.Count > 0)
            {
                current = _queue.Dequeue();

                var list = current as IEnumerable<JsValue>;
                if (list != null)
                {
                    foreach (var o in list)
                        AddItem(o);
                }
                else if (current is JsValue currentJs)
                    AddItem(currentJs);
            }

            return _engine.Array.Construct(_result.ToArray());
        }

        private void AddItem(JsValue current)
        {
            if (_results.Add(current) == false)
                return;

            _result.Add(current);
            var result = NullIfEmptyEnumerable(_func.Invoke(current));
            if (result != null)
                _queue.Enqueue(result);
        }

        private static object NullIfEmptyEnumerable(JsValue item)
        {
            if (item.IsArray() == false)
                return item;

            var itemAsArray = item.AsArray();
            if (itemAsArray.Length == 0)
                return null;

            return Yield(itemAsArray);
        }

        private static IEnumerable<JsValue> Yield(ArrayInstance array)
        {
            foreach (var item in array)
                yield return item;
        }
    }
}
