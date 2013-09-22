using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Trees
{
    public unsafe class CombinedIterator : IIterator
    {
        private readonly HashSet<Slice> deletedKeys;
        private readonly HashSet<Slice> alreadyIteratedKeys; 
        private readonly Dictionary<Slice, ReadResult> _addedValues;
        private readonly TreeIterator treeIterator;
        private Slice currentKey;
        private bool isCurrentKeyInTreeIterator;
        private Slice _requiredPrefix;
        private Slice _maxKey;
        private readonly SliceComparer _cmp;

        private Dictionary<Slice, ReadResult> AddedValues
        {
            get
            {
                return RequiredPrefix == null || String.IsNullOrWhiteSpace(RequiredPrefix.ToString())
                    ? _addedValues
                    : _addedValues.Where(kvp => kvp.Key.StartsWith(RequiredPrefix, _cmp))
                                  .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            }
        }
        
        public CombinedIterator(Dictionary<Slice, ReadResult> addedValues, HashSet<Slice> deletedKeys, TreeIterator treeIterator, SliceComparer cmp)
        {
            if (addedValues == null) throw new ArgumentNullException("addedValues");
            if (deletedKeys == null) throw new ArgumentNullException("deletedKeys");
            if (treeIterator == null) throw new ArgumentNullException("treeIterator");

            if (addedValues.Values.Any(readResult => !readResult.Stream.CanSeek))
                throw new ArgumentException("One or more of streams in added values does not support seeking. Cannot create iterator");

            alreadyIteratedKeys = new HashSet<Slice>();
            _addedValues = addedValues;
            this.deletedKeys = deletedKeys;
            this.treeIterator = treeIterator;
            isCurrentKeyInTreeIterator = true;
            _cmp = cmp;
        }

        public bool Seek(Slice key)
        {
            alreadyIteratedKeys.Clear();

            if (deletedKeys.Contains(key))
                return false;

            if (ReferenceEquals(Slice.Empty, key) || 
                ReferenceEquals(Slice.BeforeAllKeys, key) || 
                AddedValues.Count == 0 || 
                deletedKeys.Contains(key))
            {
                isCurrentKeyInTreeIterator = true;
                return treeIterator.Seek(key);
            }

            if (AddedValues.ContainsKey(key))
                isCurrentKeyInTreeIterator = false;

            if (!ReferenceEquals(Slice.AfterAllKeys,key))
                return isCurrentKeyInTreeIterator ? treeIterator.Seek(key) : SetCurrentKeyInAddedKeys(key);

            isCurrentKeyInTreeIterator = false;
            currentKey = AddedValues.Keys.LastOrDefault();

            return AddedValues.Count > 0;
        }

        public Slice CurrentKey
        {
            get
            {
                if(!isCurrentKeyInTreeIterator && currentKey == null)
                    throw new InvalidOperationException("CurrentKey was not set. Use Seek() to set the property.");

                if (AddedValues.Count == 0)
                    return treeIterator.CurrentKey;

                return (isCurrentKeyInTreeIterator) ? treeIterator.CurrentKey : currentKey;
            }
        }

        public int GetCurrentDataSize()
        {
            return isCurrentKeyInTreeIterator ? treeIterator.GetCurrentDataSize() : (int)AddedValues[currentKey].Stream.Length;
        }

        public Slice RequiredPrefix
        {
            get { return _requiredPrefix; }
            set
            {
                treeIterator.RequiredPrefix = value;
                _requiredPrefix = value; 
            }
        }

        public Slice MaxKey
        {
            get { return _maxKey; }
            set
            {
                treeIterator.MaxKey = value;
                _maxKey = value; 
            }
        }

        public bool MoveNext()
        {
            if(CurrentKey != null)
                alreadyIteratedKeys.Add(CurrentKey.Clone());

            if (AddedValues.Count == 0)
            {
                isCurrentKeyInTreeIterator = true; //in case it wasn't
                return treeIterator.MoveNext();
            }

            var hasNext = false;
            hasNext = isCurrentKeyInTreeIterator ? 
                MoveToNextTreeIteratorKey() : MoveToNextAddedValuesKey(hasNext);
            
            while (hasNext && (deletedKeys.Contains(CurrentKey) || alreadyIteratedKeys.Contains(CurrentKey)))
            {
                hasNext = isCurrentKeyInTreeIterator ? 
                    MoveToNextTreeIteratorKey() : MoveToNextAddedValuesKey(hasNext);
                
            }

            return hasNext;
        }

        private bool KeyExceedsMaxKey(Slice key)
        {
            return MaxKey != null &&                    
                   MaxKey.Compare(key, _cmp) >= 0;
        }

        private bool MoveToNextTreeIteratorKey()
        {
            var keyBeforeMoveNext = treeIterator.CurrentKey;
            var hasNext = treeIterator.MoveNext();
            if (hasNext) return true;

            var firstAddedValuesKey = AddedValues.Keys.FirstOrDefault();
            if (MaxKey != null && 
                firstAddedValuesKey != null &&
                !KeyExceedsMaxKey(firstAddedValuesKey)) return false;

            isCurrentKeyInTreeIterator = false;
            currentKey = firstAddedValuesKey;
            hasNext = AddedValues.Count > 0;
            return hasNext;
        }

        private bool MoveToNextAddedValuesKey(bool hasNext)
        {
            var addedKeys = AddedValues.Keys.ToList();
            var currentKeyIndex = addedKeys.IndexOf(currentKey);

            if ((currentKeyIndex + 1) < addedKeys.Count)
            {
                currentKey = addedKeys[currentKeyIndex + 1];
                if (KeyExceedsMaxKey(currentKey)) return false;

                hasNext = (currentKeyIndex + 1) < addedKeys.Count;
            }
            else
            {
                currentKey = null;
            }
            return hasNext;
        }

        public bool MovePrev()
        {
            bool hasPrev;
            if (AddedValues.Count == 0 || isCurrentKeyInTreeIterator)
            {
                isCurrentKeyInTreeIterator = true; //in case it wasn't
                hasPrev = MoveToPreviousTreeIteratorKey();
            }
            else
            {
                hasPrev = MoveToPreviousAddedValuesKey();
            }

            while (hasPrev && (deletedKeys.Contains(CurrentKey) || alreadyIteratedKeys.Contains(CurrentKey)))
            {
                hasPrev = isCurrentKeyInTreeIterator ? 
                    MoveToPreviousTreeIteratorKey() : MoveToPreviousAddedValuesKey();
            }

            if(hasPrev)
                alreadyIteratedKeys.Add(CurrentKey.Clone());
            return hasPrev;
        }

        private bool MoveToPreviousTreeIteratorKey()
        {
            var hasPrev = treeIterator.MovePrev();
            currentKey = hasPrev ? treeIterator.CurrentKey : null;
            return hasPrev;
        }

        private bool MoveToPreviousAddedValuesKey()
        {
            bool hasPrev;
            var addedKeys = AddedValues.Keys.ToList();
            var currentKeyIndex = addedKeys.IndexOf(currentKey);
            if (currentKeyIndex == 0)
            {
                isCurrentKeyInTreeIterator = true;
                hasPrev = treeIterator.Seek(Slice.AfterAllKeys);
            }
            else
            {
                currentKey = addedKeys[currentKeyIndex - 1];
                hasPrev = true;
            }
            return hasPrev;
        }

        public bool Skip(int count)
        {
            if (isCurrentKeyInTreeIterator)
                return treeIterator.Skip(count);

            if (AddedValues.Count == 0) return false;

            var addedKeys = AddedValues.Keys.ToList();
            var currentKeyIndex = addedKeys.IndexOf(currentKey);

            var nextKey = addedKeys.Skip(currentKeyIndex + count).FirstOrDefault();

            currentKey = nextKey ?? addedKeys.Last();

            return nextKey != null;
        }

        public Stream CreateStreamForCurrent()
        {
            if (!isCurrentKeyInTreeIterator && currentKey == null)
                return null;

            if ((isCurrentKeyInTreeIterator || AddedValues.Count == 0) && !AddedValues.ContainsKey(CurrentKey))
                return treeIterator.CreateStreamForCurrent();

            return AddedValues[CurrentKey].Stream;
        }

        public void Dispose()
        {
            if (treeIterator != null)
                treeIterator.Dispose();
        }

        private bool SetCurrentKeyInAddedKeys(Slice key)
        {
            var wasKeyAdded = AddedValues.ContainsKey(key);
            if (wasKeyAdded) currentKey = key;

            return wasKeyAdded;
        }

    }
}
