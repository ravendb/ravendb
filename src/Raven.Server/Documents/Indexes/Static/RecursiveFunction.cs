using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
//using Raven.Abstractions.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Static
{
    public class RecursiveFunction
    {
        private static readonly DynamicJsonArray Empty = new DynamicJsonArray(Enumerable.Empty<object>());

        private readonly DynamicJsonArray _result = new DynamicJsonArray();

        private readonly object _item;
        private readonly Func<object, object> _func;
        private readonly HashSet<object> _results = new HashSet<object>();
        private readonly Queue<object> _queue = new Queue<object>();

        public RecursiveFunction(object item, Func<object, object> func)
        {
            _item = item;
            _func = func;
        }

        public IEnumerable<object> Execute()
        {
            if (_item == null)
                return Empty;

            var current = NullIfEmptyEnumerable(_func(_item));
            if (current == null)
            {
                _result.Add(_item);
                return _result;
            }

            _queue.Enqueue(_item);
            while (_queue.Count > 0)
            {
                current = _queue.Dequeue();

                var list = current as IEnumerable<object>;
                if (list != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(current))
                {
                    foreach (var o in list)
                        AddItem(o);
                }
                else
                    AddItem(current);
            }

            return _result;
        }

        private void AddItem(object current)
        {
            if (_results.Add(current) == false)
                return;

            _result.Add(current);
            var result = NullIfEmptyEnumerable(_func(current));
            if (result != null)
                _queue.Enqueue(result);
        }

        private static object NullIfEmptyEnumerable(object item)
        {
            var enumerable = item as IEnumerable<object>;
            if (enumerable == null || AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(item) == false)
                return item;

            var enumerator = enumerable.GetEnumerator();
            if (enumerator.MoveNext() == false)
                return null;

            return new DynamicJsonArray(Yield(enumerator));
        }

        private static IEnumerable<object> Yield(IEnumerator<object> enumerator)
        {
            do
            {
                yield return enumerator.Current;
            } while (enumerator.MoveNext());
        }
    }
}
