using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Voron.Exceptions;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl
{
    public class WriteBatch : IDisposable
    {
        private static readonly ObjectPool<Dictionary<Slice, BatchOperation>> _lastOperationsPool = new ObjectPool<Dictionary<Slice, BatchOperation>>(() => new Dictionary<Slice, BatchOperation>(SliceComparer.Instance), 50);
        private static readonly ObjectPool<Dictionary<Slice, List<BatchOperation>>> _multiTreeOperationsPool = new ObjectPool<Dictionary<Slice, List<BatchOperation>>>(() => new Dictionary<Slice, List<BatchOperation>>(SliceComparer.Instance), 50);

        private readonly Dictionary<string, Dictionary<Slice, BatchOperation>> _lastOperations;
        private readonly Dictionary<string, Dictionary<Slice, List<BatchOperation>>> _multiTreeOperations;

        private readonly HashSet<string> _trees = new HashSet<string>();

        private bool _disposeAfterWrite = true;
        private volatile bool _disposed = false;

        public HashSet<string> Trees
        {
            get
            {
                return _trees;
            }
        }

        internal IEnumerable<BatchOperation> GetSortedOperations(string treeName)
        {
            Dictionary<Slice, BatchOperation> operations;
            if (_lastOperations.TryGetValue(treeName, out operations))
            {
                foreach (var operation in operations.OrderBy(x => x.Key, SliceComparer.Instance))
                    yield return operation.Value;
            }

            if (_multiTreeOperations.Count == 0)
                yield break;

            Dictionary<Slice, List<BatchOperation>> multiOperations;
            if (_multiTreeOperations.TryGetValue(treeName, out multiOperations) == false)
                yield break;

            var orderedOperations = multiOperations
                                        .SelectMany(x => x.Value)
                                        .OrderBy(x => x.ValueSlice, SliceComparer.Instance)
                                        .ThenBy(x => x.Key, SliceComparer.Instance);

            foreach (var operation in orderedOperations)
                yield return operation;
        }

        private long totalSize = 0;

        public long Size()
        {
            return totalSize;
        }

        public bool IsEmpty { get { return _lastOperations.Count == 0 && _multiTreeOperations.Count == 0; } }

        public bool DisposeAfterWrite
        {
            get { return _disposeAfterWrite; }
            set { _disposeAfterWrite = value; }
        }

        internal bool TryGetValue(string treeName, Slice key, out InBatchValue result)
        {
            result = new InBatchValue()
            {
                OperationType = BatchOperationType.None
            };

            if (treeName == null)
                treeName = Constants.RootTreeName;

            //first check if it is a multi-tree operation
            Dictionary<Slice, List<BatchOperation>> treeOperations;
            if (_multiTreeOperations.TryGetValue(treeName, out treeOperations))
            {
                List<BatchOperation> operationRecords;
                if (treeOperations.TryGetValue(key, out operationRecords))
                {
                    //since in multi-tree there are many operations for single tree key, then fetching operation type and value is meaningless
                    return true;
                }
            }

            Dictionary<Slice, BatchOperation> operations;
            if (_lastOperations.TryGetValue(treeName, out operations) == false)
                return false;

            BatchOperation operation;
            if (operations.TryGetValue(key, out operation))
            {
                result.OperationType = operation.Type;
                result.Version = operation.Version;

                if (operation.Type == BatchOperationType.Delete)
                    return true;

                result.Stream = operation.ValueStream;
                operation.Reset(); // will reset stream position
                result.Struct = operation.ValueStruct;

                if (operation.Type == BatchOperationType.Add || operation.Type == BatchOperationType.AddStruct)
                    return true;
            }

            return false;
        }

        public WriteBatch()
        {
            _lastOperations = new Dictionary<string, Dictionary<Slice, BatchOperation>>();
            _multiTreeOperations = new Dictionary<string, Dictionary<Slice, List<BatchOperation>>>();
        }

        public void Add(Slice key, Slice value, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertValidTreeName(treeName);
            if (value == null) throw new ArgumentNullException("value");

            var batchOperation = BatchOperation.Add(key, value, version, treeName);
            if (shouldIgnoreConcurrencyExceptions)
                batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
            AddOperation(batchOperation);
        }

        public void Add(Slice key, Stream value, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertValidTreeName(treeName);
            if (value == null) throw new ArgumentNullException("value");
            if (value.Length > int.MaxValue)
                throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

            var batchOperation = BatchOperation.Add(key, value, version, treeName);
            if (shouldIgnoreConcurrencyExceptions)
                batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
            AddOperation(batchOperation);
        }

        public void AddStruct(Slice key, IStructure value, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            var batchOperation = BatchOperation.Add(key, value, version, treeName);
            if (shouldIgnoreConcurrencyExceptions)
                batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
            AddOperation(batchOperation);
        }

        public void Delete(Slice key, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertValidRemove(treeName);

            var batchOperation = BatchOperation.Delete(key, version, treeName);
            if (shouldIgnoreConcurrencyExceptions)
                batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
            AddOperation(batchOperation);
        }

        private static void AssertValidRemove(string treeName)
        {
            AssertValidTreeName(treeName);
        }

        public void MultiAdd(Slice key, Slice value, string treeName, ushort? version = null)
        {
            AssertValidMultiOperation(value, treeName);

            AddOperation(BatchOperation.MultiAdd(key, value, version, treeName));
        }

        public void MultiDelete(Slice key, Slice value, string treeName, ushort? version = null)
        {
            AssertValidMultiOperation(value, treeName);

            AddOperation(BatchOperation.MultiDelete(key, value, version, treeName));
        }

        public void Increment(Slice key, long delta, string treeName, ushort? version = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertValidTreeName(treeName);

            var batchOperation = BatchOperation.Increment(key, delta, version, treeName);
            if (shouldIgnoreConcurrencyExceptions)
                batchOperation.SetIgnoreExceptionOnExecution<ConcurrencyException>();
            AddOperation(batchOperation);
        }

        private static void AssertValidTreeName(string treeName)
        {
            if (treeName != null && treeName.Length == 0) 
                throw new ArgumentException("treeName must not be empty", "treeName");
        }

        private static void AssertValidMultiOperation(Slice value, string treeName)
        {
            AssertValidTreeName(treeName);
            if (value == null) throw new ArgumentNullException("value");
            if (value.Size == 0)
                throw new ArgumentException("Cannot add empty value");
        }

        private void AddOperation(BatchOperation operation)
        {
            var treeName = operation.TreeName;
            AssertValidTreeName(treeName);
            AssertValidKey(operation.Key);

            if (treeName == null)
                treeName = Constants.RootTreeName;

            _trees.Add(treeName);

            if (operation.Type == BatchOperationType.MultiAdd || operation.Type == BatchOperationType.MultiDelete)
            {
                Dictionary<Slice, List<BatchOperation>> multiTreeOperationsOfTree;
                if (_multiTreeOperations.TryGetValue(treeName, out multiTreeOperationsOfTree) == false)
                {
                    _multiTreeOperations[treeName] = multiTreeOperationsOfTree = _multiTreeOperationsPool.Allocate();
                    Debug.Assert(multiTreeOperationsOfTree.Count == 0);
                }

                List<BatchOperation> specificMultiTreeOperations;
                if (multiTreeOperationsOfTree.TryGetValue(operation.Key, out specificMultiTreeOperations) == false)
                    multiTreeOperationsOfTree[operation.Key] = specificMultiTreeOperations = new List<BatchOperation>();

                specificMultiTreeOperations.Add(operation);

                totalSize += operation.Key.Size;
            }
            else
            {
                Dictionary<Slice, BatchOperation> lastOpsForTree;
                if (_lastOperations.TryGetValue(treeName, out lastOpsForTree) == false)
                {
                    _lastOperations[treeName] = lastOpsForTree = _lastOperationsPool.Allocate();
                    Debug.Assert(lastOpsForTree.Count == 0); // Make sure we didn't mess up when cleaning up.
                }

                BatchOperation old;
                if (lastOpsForTree.TryGetValue(operation.Key, out old))
                {
                    operation.SetVersionFrom(old);

                    if (operation.Type == BatchOperationType.Add)
                        totalSize -= operation.Key.Size + operation.ValueSize;
                    else
                        totalSize -= operation.Key.Size;

                    if (old.ValueStream != null)
                        old.ValueStream.Dispose();                    
                }

                lastOpsForTree[operation.Key] = operation;

                if (operation.Type == BatchOperationType.Add)
                    totalSize += operation.Key.Size + operation.ValueSize;
                else
                    totalSize += operation.Key.Size;
            }
        }

        private void AssertValidKey(Slice key)
        {
            if (key.Size + Constants.NodeHeaderSize > AbstractPager.NodeMaxSize)
                throw new ArgumentException(
                    "Key size is too big, must be at most " + (AbstractPager.NodeMaxSize - Constants.NodeHeaderSize) + " bytes, but was " + key.Size, "key");
        }

        internal class BatchOperation : IComparable<BatchOperation>
        {
            private readonly long _originalStreamPosition = -1;			
            private HashSet<Type> _exceptionTypesToIgnore;

            public readonly Stream ValueStream;
            public readonly Slice ValueSlice;
            public readonly IStructure ValueStruct;

            public readonly long ValueLong;

            public static BatchOperation Add(Slice key, Slice value, ushort? version, string treeName)
            {
                return new BatchOperation(key, value, version, treeName, BatchOperationType.Add);
            }

            public static BatchOperation Add(Slice key, Stream stream, ushort? version, string treeName)
            {
                return new BatchOperation(key, stream, version, treeName, BatchOperationType.Add);
            }

            public static BatchOperation Add(Slice key, IStructure value, ushort? version, string treeName)
            {
                return new BatchOperation(key, value, version, treeName, BatchOperationType.AddStruct);
            }

            public static BatchOperation Delete(Slice key, ushort? version, string treeName)
            {
                return new BatchOperation(key, null as Stream, version, treeName, BatchOperationType.Delete);
            }

            public static BatchOperation MultiAdd(Slice key, Slice value, ushort? version, string treeName)
            {
                return new BatchOperation(key, value, version, treeName, BatchOperationType.MultiAdd);
            }

            public static BatchOperation MultiDelete(Slice key, Slice value, ushort? version, string treeName)
            {
                return new BatchOperation(key, value, version, treeName, BatchOperationType.MultiDelete);
            }

            public static BatchOperation Increment(Slice key, long delta, ushort? version, string treeName)
            {
                return new BatchOperation(key, delta, version, treeName);
            }

            private BatchOperation(Slice key, Stream value, ushort? version, string treeName, BatchOperationType type)
                : this(key, version, treeName, type)
            {
                if (value != null)
                {
                    _originalStreamPosition = value.Position;
                    ValueSize = value.Length;
                }

                ValueStream = value;
            }

            private BatchOperation(Slice key, Slice value, ushort? version, string treeName, BatchOperationType type)
                : this(key, version, treeName, type)
            {
                if (value != null)
                    ValueSize = value.Size;

                ValueSlice = value;
            }

            private BatchOperation(Slice key, IStructure value, ushort? version, string treeName, BatchOperationType type)
                : this(key, version, treeName, type)
            {
                ValueStruct = value;
            }

            private BatchOperation(Slice key, long value, ushort? version, string treeName)
                : this(key, version, treeName, BatchOperationType.Increment)
            {
                ValueLong = value;
            }

            private BatchOperation(Slice key, ushort? version, string treeName, BatchOperationType type)
            {
                Key = key;
                Version = version;
                TreeName = treeName;
                Type = type;
            }

            public readonly Slice Key;

            public readonly long ValueSize;

            public readonly string TreeName;

            public readonly BatchOperationType Type;

            public ushort? Version { get; private set; }

            public HashSet<Type> ExceptionTypesToIgnore
            {
                get 
                {
                    if ( _exceptionTypesToIgnore == null )
                        _exceptionTypesToIgnore = new HashSet<Type>();
                    return _exceptionTypesToIgnore; 
                }
            }

            public void SetVersionFrom(BatchOperation other)
            {
                if (other.Version != null &&
                    other.Version + 1 == Version)
                    Version = other.Version;
            }

            public void Reset()
            {
                if (_originalStreamPosition != -1)
                    ValueStream.Position = _originalStreamPosition;
            }

            public void SetIgnoreExceptionOnExecution<T>()
                where T : Exception
            {
                ExceptionTypesToIgnore.Add(typeof(T));
            }

            public int CompareTo(BatchOperation other)
            {
                var r = SliceComparer.CompareInline(Key, other.Key);
                if (r != 0)
                    return r;

                if (ValueSlice != null)
                {
                    if (other.ValueSlice == null)
                        return -1;

                    return SliceComparer.CompareInline(ValueSlice, other.ValueSlice);
                }
                else if (other.ValueSlice != null)
                {
                    return 1;
                }
                return 0;
            }

            public object GetValueForDebugJournal()
            {
                if (Type == BatchOperationType.Increment)
                    return ValueLong;

                if (ValueStream != null)
                    return ValueStream;

                if (ValueSlice != null)
                    return ValueSlice;

                if (ValueStruct != null)
                    return ValueStruct;

                return null;
            }
        }

        public enum BatchOperationType
        {
            None = 0,
            Add = 1,
            Delete = 2,
            MultiAdd = 3,
            MultiDelete = 4,
            Increment = 5,
            AddStruct = 6,
        }

        public class InBatchValue
        {
            public Stream Stream;
            public IStructure Struct;
            public ushort? Version;
            public BatchOperationType OperationType;
        }

        public void Dispose()
        {
            // Guard to avoid multiple disposing scenarios. 
            if (_disposed == false )
            {
                _disposed = true;

                foreach (var operation in _lastOperations)
                {
                    foreach (var val in operation.Value)
                    {
                        if (val.Value.ValueStream == null)
                            continue;

                        val.Value.ValueStream.Dispose();
                    }

                    operation.Value.Clear();
                    _lastOperationsPool.Free(operation.Value);

                }

                foreach (var item in _multiTreeOperations)
                {
                    item.Value.Clear();
                    _multiTreeOperationsPool.Free(item.Value);
                }

                _lastOperations.Clear();
            }
        }
    }
}
