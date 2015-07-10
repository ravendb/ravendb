using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Collections
{
    public class ZFastTrieSortedSet<TKey, TValue>
    {
        protected abstract class Node<TKey, TValue>
        {
            public abstract bool IsLeaf { get; }
            public abstract bool IsInternal { get; }
        }

        protected class Leaf<TKey, TValue> : Node<TKey, TValue>
        {
            public override bool IsLeaf { get { return true; } }
            public override bool IsInternal { get { return false; } }
        }

        protected class Internal<TKey, TValue> : Node<TKey, TValue>
        {
            public override bool IsLeaf { get { return false; } }
            public override bool IsInternal { get { return true; } }
        }

        

        private Leaf<TKey, TValue> head;
        private Leaf<TKey, TValue> tail;


        public ZFastTrieSortedSet(IEnumerable<KeyValuePair<TKey, TValue>> elements)
        {
            Add(elements);
        }

        public int Count { get; private set; }

        public void Add(TKey key, TValue value)
        {
            throw new NotImplementedException();
        }

        public void Add(IEnumerable<KeyValuePair<TKey, TValue>> elements)
        {
            throw new NotImplementedException();
        }

        public void Remove(TKey key, TValue value)
        {
            throw new NotImplementedException();
        }

        public void Remove(IEnumerable<KeyValuePair<TKey, TValue>> elements)
        {
            throw new NotImplementedException();
        }

        public TKey Successor(TKey key)
        {
            throw new NotImplementedException();
        }

        public TKey Predecessor(TKey key)
        {
            throw new NotImplementedException();
        }

        public TKey LastKeyOrDefault()
        {
            throw new NotImplementedException();
        }

        public TKey FirstKeyOrDefault()
        {
            throw new NotImplementedException();
        }
    }
}
