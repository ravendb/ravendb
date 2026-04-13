using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public partial class ConversationDocument
{
    /// <summary>
    /// Wraps conversation messages with lazy materialization. When constructed from a
    /// BlittableJsonReaderArray (via ToDocument), provides zero-copy read access.
    /// On first mutation (Add, RemoveRange, Clear), materializes to an internal list.
    /// Points back to the owning ConversationDocument for its backing storage.
    /// </summary>
    public struct MessagesList : IList<BlittableJsonReaderObject>
    {
        private readonly ConversationDocument _owner;

        internal MessagesList(ConversationDocument owner) => _owner = owner;

        public int Count => _owner._messagesList?.Count ?? _owner._messagesArray?.Length ?? 0;

        public bool IsReadOnly => false;

        public BlittableJsonReaderObject this[int index]
        {
            get => _owner._messagesList != null ? _owner._messagesList[index] : (BlittableJsonReaderObject)_owner._messagesArray[index];
            set => Materialize()[index] = value;
        }

        public void Add(BlittableJsonReaderObject msg) => Materialize().Add(msg);

        public void Insert(int index, BlittableJsonReaderObject item) => Materialize().Insert(index, item);

        public bool Remove(BlittableJsonReaderObject item) => Materialize().Remove(item);

        public void RemoveAt(int index) => Materialize().RemoveAt(index);

        public void RemoveRange(int index, int count) => Materialize().RemoveRange(index, count);

        public void Clear()
        {
            _owner._messagesArray = null;
            _owner._messagesList = [];
        }

        public bool Contains(BlittableJsonReaderObject item)
        {
            for (int i = 0; i < Count; i++)
                if (this[i] == item) return true;
            return false;
        }

        public int IndexOf(BlittableJsonReaderObject item)
        {
            for (int i = 0; i < Count; i++)
                if (this[i] == item) return i;
            return -1;
        }

        public void CopyTo(BlittableJsonReaderObject[] array, int arrayIndex)
        {
            for (int i = 0; i < Count; i++)
                array[arrayIndex + i] = this[i];
        }

        public BlittableJsonReaderObject FirstOrDefault() => Count == 0 ? null : this[0];

        public BlittableJsonReaderObject LastOrDefault() => Count == 0 ? null : this[Count - 1];

        public IEnumerable<BlittableJsonReaderObject> Skip(int count)
        {
            for (int i = count; i < Count; i++)
                yield return this[i];
        }

        /// <summary>
        /// Returns a struct enumerator for allocation-free foreach.
        /// </summary>
        public Enumerator GetEnumerator() => new(this);

        IEnumerator<BlittableJsonReaderObject> IEnumerable<BlittableJsonReaderObject>.GetEnumerator() => new Enumerator(this);

        IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

        public struct Enumerator : IEnumerator<BlittableJsonReaderObject>
        {
            private readonly MessagesList _messages;
            private int _index;

            internal Enumerator(MessagesList messages)
            {
                _messages = messages;
                _index = -1;
            }

            public BlittableJsonReaderObject Current => _messages[_index];

            object IEnumerator.Current => Current;

            public bool MoveNext() => ++_index < _messages.Count;

            public void Reset() => _index = -1;

            public void Dispose() { }
        }

        /// <summary>
        /// Returns the underlying storage in a form that ObjectJsonParser can serialize
        /// (either List or BlittableJsonReaderArray — both natively supported).
        /// </summary>
        internal object AsSerializable() => (object)_owner._messagesList ?? _owner._messagesArray;

        private List<BlittableJsonReaderObject> Materialize()
        {
            if (_owner._messagesList != null)
                return _owner._messagesList;

            var array = _owner._messagesArray;
            var list = new List<BlittableJsonReaderObject>(array?.Length ?? 0);
            if (array != null)
            {
                for (int i = 0; i < array.Length; i++)
                    list.Add(((BlittableJsonReaderObject)array[i]).CloneOnTheSameContext());
            }

            _owner._messagesList = list;
            _owner._messagesArray = null;
            return list;
        }
    }
}
