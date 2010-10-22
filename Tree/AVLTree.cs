using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Munin.Tree
{
    // Code originated from
    // http://blogs.msdn.com/b/ericlippert/archive/2008/01/21/immutability-in-c-part-nine-academic-plus-my-avl-tree-implementation.aspx
    public sealed class AVLTree<TKey, TValue> : IBinarySearchTree<TKey, TValue>
    {
        private readonly int height;

        public override string ToString()
        {
            return string.Format("Key: {0}, Value: {1}, Count: {2}", theKey, theValue, Count);
        }

        public JObject ToJObject()
        {
            return new JObject
            {
                {"key", JToken.FromObject(theKey)},
                {"value", JToken.FromObject(theValue)},
                {"left", Left.ToJObject()},
                {"right", Right.ToJObject()}
            };
        }

        private readonly IComparer<TKey> comparer;

        private readonly TKey theKey;
        private readonly IBinarySearchTree<TKey, TValue> left;
        private readonly IBinarySearchTree<TKey, TValue> right;
        private readonly TValue theValue;
        public int Count { get; private set; }
        internal AVLTree(IComparer<TKey> comparer, TKey key, TValue value, IBinarySearchTree<TKey, TValue> left, IBinarySearchTree<TKey, TValue> right)
        {
            this.comparer = comparer;
            this.theKey = key;
            this.theValue = value;
            this.left = left;
            this.right = right;
            height = 1 + Math.Max(Height(left), Height(right));
            Count = 1 + Left.Count + Right.Count;
        }

        // IBinaryTree

        #region IBinarySearchTree<TKey,TValue> Members

        public IComparer<TKey> Comparer
        {
            get { return comparer; }
        }

        public bool IsEmpty
        {
            get { return false; }
        }

        public TValue Value
        {
            get { return theValue; }
        }

        public IBinarySearchTree<TKey, TValue> LeftMost
        {
            get
            {
                if(Left.IsEmpty)
                    return this;
                var current = Left;
                while (current.Left.IsEmpty == false)
                    current = current.Left;
                return current.LeftMost;
            }
        }

        public IBinarySearchTree<TKey, TValue> RightMost
        {
            get
            {
                if (Right.IsEmpty)
                    return this;
                var current = Right;
                while (current.Right.IsEmpty == false)
                    current = current.Right;
                return current.RightMost;
            }
        }

        // IBinarySearchTree
        public IBinarySearchTree<TKey, TValue> Left
        {
            get { return left; }
        }

        public IBinarySearchTree<TKey, TValue> Right
        {
            get { return right; }
        }

        public IBinarySearchTree<TKey, TValue> Search(TKey key)
        {
            int compare = comparer.Compare(key, Key);
            if (compare == 0)
                return this;
            if (compare > 0)
                return Right.Search(key);
            return Left.Search(key);
        }

        public IEnumerable<TValue> GreaterThan(TKey gtKey)
        {
            int compare = comparer.Compare(Key, gtKey);
            if (compare <= 0)
            {
                foreach (var value in Right.GreaterThan(gtKey))
                {
                    yield return value;
                }
                yield break;
            }
            foreach (var value in Left.GreaterThan(gtKey))
            {
                yield return value;
            }
            yield return Value;
            foreach (var value in Right.GreaterThan(gtKey))
            {
                yield return value;
            }
        }

        public IEnumerable<TValue> GreaterThanOrEqual(TKey gteKey)
        {
            int compare = comparer.Compare(Key, gteKey);
            if (compare < 0)
            {
                foreach (var value in Right.GreaterThanOrEqual(gteKey))
                {
                    yield return value;
                } 
                yield break;
            }

            foreach (var value in Left.GreaterThanOrEqual(gteKey))
            {
                yield return value;
            } 
            yield return Value;
            foreach (var value in Right.GreaterThanOrEqual(gteKey))
            {
                yield return value;
            }
        }


        public TKey Key
        {
            get { return theKey; }
        }

        public IBinarySearchTree<TKey, TValue> Add(TKey key, TValue value)
        {
            AVLTree<TKey, TValue> result;
            if (comparer.Compare(key, Key) > 0)
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left, Right.Add(key, value));
            else
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left.Add(key, value), Right);
            return MakeBalanced(result);
        }

        public IBinarySearchTree<TKey, TValue> AddOrUpdate(TKey key, TValue value, Func<TKey, TValue, TValue> updateValueFactory)
        {
            AVLTree<TKey, TValue> result;
            var compare = comparer.Compare(key, Key);
            if (compare > 0)
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left, Right.AddOrUpdate(key, value, updateValueFactory));
            else if(compare < 0)
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left.AddOrUpdate(key, value, updateValueFactory), Right);
            else
            {
                var newValue = updateValueFactory(key, Value);
                result = new AVLTree<TKey, TValue>(comparer, key, newValue, Left, Right);
            }
            return MakeBalanced(result);
        }

        public IBinarySearchTree<TKey, TValue> TryRemove(TKey key, out bool removed, out TValue value)
        {
            IBinarySearchTree<TKey, TValue> result;
            int compare = comparer.Compare(key, Key);
            if (compare == 0)
            {
                removed = true;
                value = Value;
                // We have a match. If this is a leaf, just remove it 
                // by returning Empty.  If we have only one child,
                // replace the node with the child.
                if (Right.IsEmpty && Left.IsEmpty)
                    result = new EmptyAVLTree<TKey, TValue>(comparer);
                else if (Right.IsEmpty && !Left.IsEmpty)
                    result = Left;
                else if (!Right.IsEmpty && Left.IsEmpty)
                    result = Right;
                else
                {
                    // We have two children. Remove the next-highest node and replace
                    // this node with it.
                    IBinarySearchTree<TKey, TValue> successor = Right;
                    while (!successor.Left.IsEmpty)
                        successor = successor.Left;
                    result = new AVLTree<TKey, TValue>(comparer, successor.Key, successor.Value, Left, Right.TryRemove(successor.Key, out removed, out value));
                }
            }
            else if (compare < 0)
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left.TryRemove(key, out removed, out value), Right);
            else
                result = new AVLTree<TKey, TValue>(comparer, Key, Value, Left, Right.TryRemove(key, out removed, out value));
            return MakeBalanced(result);
        }

        // IMap
        public bool Contains(TKey key)
        {
            return !Search(key).IsEmpty;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = default(TValue);
            IBinarySearchTree<TKey, TValue> tree = Search(key);
            if (tree.IsEmpty)
                return false;
            value = tree.Value;
            return true;
        }

        public IEnumerable<TKey> KeysInOrder
        {
            get { return from t in EnumerateInOrder() select t.Key; }
        }

        public IEnumerable<TValue> ValuesInOrder
        {
            get { return from t in EnumerateInOrder() select t.Value; }
        }

        public IEnumerable<TValue> ValuesInReverseOrder
        {
            get { return from t in EnumerateInReverseOrder() select t.Value; }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> Pairs
        {
            get { return from t in EnumerateInOrder() select new KeyValuePair<TKey, TValue>(t.Key, t.Value); }
        }

        #endregion

        private IEnumerable<IBinarySearchTree<TKey, TValue>> EnumerateInOrder()
        {
            var stack = Stack<IBinarySearchTree<TKey, TValue>>.Empty;
            for (IBinarySearchTree<TKey, TValue> current = this; !current.IsEmpty || !stack.IsEmpty; current = current.Right)
            {
                while (!current.IsEmpty)
                {
                    stack = stack.Push(current);
                    current = current.Left;
                }
                current = stack.Peek();
                stack = stack.Pop();
                yield return current;
            }
        }

        private IEnumerable<IBinarySearchTree<TKey, TValue>> EnumerateInReverseOrder()
        {
            var stack = Stack<IBinarySearchTree<TKey, TValue>>.Empty;
            for (IBinarySearchTree<TKey, TValue> current = this; !current.IsEmpty || !stack.IsEmpty; current = current.Left)
            {
                while (!current.IsEmpty)
                {
                    stack = stack.Push(current);
                    current = current.Right;
                }
                current = stack.Peek();
                stack = stack.Pop();
                yield return current;
            }
        }

        // Static helpers for tree balancing
        private static int Height(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.IsEmpty)
                return 0;
            return ((AVLTree<TKey, TValue>)tree).height;
        }

        private static IBinarySearchTree<TKey, TValue> RotateLeft(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.Right.IsEmpty)
                return tree;
            return new AVLTree<TKey, TValue>(tree.Comparer, tree.Right.Key, tree.Right.Value,
                                     new AVLTree<TKey, TValue>(tree.Comparer, tree.Key, tree.Value, tree.Left, tree.Right.Left),
                                     tree.Right.Right);
        }

        private static IBinarySearchTree<TKey, TValue> RotateRight(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.Left.IsEmpty)
                return tree;
            return new AVLTree<TKey, TValue>(tree.Comparer, tree.Left.Key, tree.Left.Value, tree.Left.Left,
                                     new AVLTree<TKey, TValue>(tree.Comparer, tree.Key, tree.Value, tree.Left.Right, tree.Right));
        }

        private static IBinarySearchTree<TKey, TValue> DoubleLeft(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.Right.IsEmpty)
                return tree;
            var rotatedRightChild = new AVLTree<TKey, TValue>(tree.Comparer, tree.Key, tree.Value, tree.Left, RotateRight(tree.Right));
            return RotateLeft(rotatedRightChild);
        }

        private static IBinarySearchTree<TKey, TValue> DoubleRight(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.Left.IsEmpty)
                return tree;
            var rotatedLeftChild = new AVLTree<TKey, TValue>(tree.Comparer, tree.Key, tree.Value, RotateLeft(tree.Left), tree.Right);
            return RotateRight(rotatedLeftChild);
        }

        private static int Balance(IBinarySearchTree<TKey, TValue> tree)
        {
            if (tree.IsEmpty)
                return 0;
            return Height(tree.Right) - Height(tree.Left);
        }

        private static bool IsRightHeavy(IBinarySearchTree<TKey, TValue> tree)
        {
            return Balance(tree) >= 2;
        }

        private static bool IsLeftHeavy(IBinarySearchTree<TKey, TValue> tree)
        {
            return Balance(tree) <= -2;
        }

        private static IBinarySearchTree<TKey, TValue> MakeBalanced(IBinarySearchTree<TKey, TValue> tree)
        {
            IBinarySearchTree<TKey, TValue> result;
            if (IsRightHeavy(tree))
            {
                if (IsLeftHeavy(tree.Right))
                    result = DoubleLeft(tree);
                else
                    result = RotateLeft(tree);
            }
            else if (IsLeftHeavy(tree))
            {
                if (IsRightHeavy(tree.Left))
                    result = DoubleRight(tree);
                else
                    result = RotateRight(tree);
            }
            else
                result = tree;
            return result;
        }

        #region Nested type: EmptyAVLTree

        #endregion
    }
}