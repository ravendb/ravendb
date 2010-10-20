using System;
using System.Collections.Generic;

namespace Raven.Munin.Tree
{
    internal sealed class EmptyAVLTree<TKey, TValue> : IBinarySearchTree<TKey, TValue>
    {
        private IComparer<TKey> comparer;

        public EmptyAVLTree(IComparer<TKey> comparer)
        {
            this.comparer = comparer;
        }

        // IBinaryTree

        #region IBinarySearchTree<K,TValue> Members

        public IComparer<TKey> Comparer
        {
            get { return comparer; }
        }

        public int Count
        {
            get { return 0; }
        }

        public bool IsEmpty
        {
            get { return true; }
        }

        public TValue Value
        {
            get { throw new Exception("empty tree"); }
        }

        // IBinarySearchTree
        public IBinarySearchTree<TKey, TValue> Left
        {
            get { throw new Exception("empty tree"); }
        }

        public IBinarySearchTree<TKey, TValue> Right
        {
            get { throw new Exception("empty tree"); }
        }

        public IBinarySearchTree<TKey, TValue> Search(TKey key)
        {
            return this;
        }

        public TKey Key
        {
            get { throw new Exception("empty tree"); }
        }

        public IBinarySearchTree<TKey, TValue> Add(TKey key, TValue value)
        {
            return new AVLTree<TKey, TValue>(comparer, key, value, this, this);
        }

        public IBinarySearchTree<TKey, TValue> AddOrUpdate(TKey key, TValue value, Func<TKey, TValue, TValue> updateValueFactory)
        {
            // we don't udpate, so we don't care about the update value factory
            return new AVLTree<TKey, TValue>(comparer, key, value, this, this);
        }

        public IBinarySearchTree<TKey, TValue> TryRemove(TKey key, out bool removed, out TValue value)
        {
            removed = false;
            value = default(TValue);
            return this;
        }

        // IMap
        public bool Contains(TKey key)
        {
            return false;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = default(TValue);
            return false;
        }

        public IEnumerable<TKey> Keys
        {
            get { yield break; }
        }

        public IEnumerable<TValue> Values
        {
            get { yield break; }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> Pairs
        {
            get { yield break; }
        }

        #endregion
    }
}