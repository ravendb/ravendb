using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public partial class ConversationDocument
{
    /// <summary>
    /// Wraps conversation messages with copy-on-write semantics. Two construction modes:
    /// <list type="bullet">
    /// <item><description><b>Owned</b> (<c>cloneArray = true</c>, default): clones the source array upfront so the instance
    /// is independent of the parent document's lifetime. Required for write flows where the source
    /// read transaction closes before mutations happen.</description></item>
    /// <item><description><b>Borrowed</b> (<c>cloneArray = false</c>): keeps the source array reference — true zero-copy.
    /// The caller must keep the parent document alive for as long as this instance is used.
    /// Suitable for read-only flows that hold the source transaction open throughout.</description></item>
    /// </list>
    /// On first mutation (Add, RemoveRange, Clear), individual messages are cloned onto the source
    /// context and switched to an internal list.
    /// </summary>
    public sealed class MessagesList : IList<BlittableJsonReaderObject>
    {
        private BlittableJsonReaderArray _messagesArray;
        private List<BlittableJsonReaderObject> _messagesList;
        private readonly bool _ownsArray;

        internal MessagesList() { }

        internal MessagesList(BlittableJsonReaderArray array, bool cloneArray = true)
        {
            // Owned mode clones at the same context so the source document can be disposed safely.
            // Borrowed mode keeps the reference verbatim — caller is responsible for lifetime.
            _messagesArray = cloneArray ? array.Clone() : array;
            _ownsArray = cloneArray;
        }

        public int Count => _messagesList?.Count ?? _messagesArray?.Length ?? 0;

        public bool IsReadOnly => false;

        public BlittableJsonReaderObject this[int index]
        {
            get => _messagesList != null ? _messagesList[index] : (BlittableJsonReaderObject)_messagesArray[index];
            set => Materialize()[index] = value;
        }

        public void Add(BlittableJsonReaderObject msg) => Materialize().Add(msg);

        public void Insert(int index, BlittableJsonReaderObject item) => Materialize().Insert(index, item);

        public bool Remove(BlittableJsonReaderObject item) => Materialize().Remove(item);

        public void RemoveAt(int index) => Materialize().RemoveAt(index);

        public void RemoveRange(int index, int count) => Materialize().RemoveRange(index, count);

        public void Clear()
        {
            if (_ownsArray)
                _messagesArray?.Dispose();
            _messagesArray = null;
            _messagesList = [];
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
        internal object AsSerializable() => (object)_messagesList ?? _messagesArray;

        private List<BlittableJsonReaderObject> Materialize()
        {
            if (_messagesList != null)
                return _messagesList;

            var array = _messagesArray;
            var list = new List<BlittableJsonReaderObject>(array?.Length ?? 0);
            if (array != null)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    if (array[i] is BlittableJsonReaderObject oldMessage)
                        list.Add(oldMessage.CloneOnTheSameContext());
                }
            }

            _messagesList = list;
            if (_ownsArray)
                _messagesArray?.Dispose();
            _messagesArray = null;
            return list;
        }
    }
}
