using System;
using System.Collections;
using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Voron.Data.Tables
{
    public sealed unsafe partial class Table
    {
        private enum SeekState : byte
        {
            NotStarted,
            Running,
            Done
        }

        public readonly struct PrimaryKeyPrefixSeek(Table table, Slice requiredPrefix, Slice startAfter, long skip) : IEnumerable<(Slice Key, TableValueHolder Value)>
        {
            public Enumerator GetEnumerator() => new(table, requiredPrefix, startAfter, skip);

            // a leaf at a time, from the keys alone: the rows are not read
            public long Count()
            {
                using var it = table.GetTree(table._schema.Key).Iterate(prefetch: false);
                if (SeekFirst(it, requiredPrefix, startAfter, skip: 0) == false)
                    return 0;

                return Math.Max(0, it.CountToEndOfRequiredPrefix() - skip);
            }

            // positions the iterator on the first result: the first key after startAfter when given, then skip results on
            private static bool SeekFirst(TreeIterator it, Slice requiredPrefix, Slice startAfter, long skip)
            {
                it.SetRequiredPrefix(requiredPrefix);

                if (IsEmptyMarker(startAfter))
                {
                    if (it.Seek(requiredPrefix) == false)
                        return false;
                }
                else
                {
                    if (it.Seek(startAfter) == false)
                        return false;

                    // startAfter itself is not a result
                    if (SliceComparer.Equals(it.CurrentKey, startAfter) && it.MoveNext() == false)
                        return false;
                }

                return it.Skip(skip);
            }

            IEnumerator<(Slice Key, TableValueHolder Value)> IEnumerable<(Slice Key, TableValueHolder Value)>.GetEnumerator() => GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public struct Enumerator(Table table, Slice requiredPrefix, Slice startAfter, long skip) : IEnumerator<(Slice Key, TableValueHolder Value)>
            {
                private TreeIterator _it;
                private TableValueHolder _result;
                private SeekState _state;

                public (Slice Key, TableValueHolder Value) Current { get; private set; }

                object IEnumerator.Current => Current;

                public bool MoveNext()
                {
                    switch (_state)
                    {
                        case SeekState.NotStarted:
                            _state = SeekState.Running;

                            _it = table.GetTree(table._schema.Key).Iterate(table._prefetch);
                            if (SeekFirst(_it, requiredPrefix, startAfter, skip) == false)
                                return Finish();

                            _result = new TableValueHolder();
                            break;

                        case SeekState.Running:
                            if (_it.MoveNext() == false)
                                return Finish();
                            break;

                        default:
                            return false;
                    }

                    table.GetTableValueReader(_it, out _result.Reader);
                    Current = (_it.CurrentKey, _result);
                    return true;
                }

                private bool Finish()
                {
                    Dispose();
                    return false;
                }

                public void Dispose()
                {
                    _state = SeekState.Done;
                    _it?.Dispose();
                    _it = null;
                }

                public void Reset() => throw new NotSupportedException();
            }
        }

        public readonly struct PrimaryKeySeek(Table table, Slice value, long skip, bool backward) : IEnumerable<TableValueHolder>
        {
            public Enumerator GetEnumerator() => new(table, value, skip, backward);

            IEnumerator<TableValueHolder> IEnumerable<TableValueHolder>.GetEnumerator() => GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public struct Enumerator(Table table, Slice value, long skip, bool backward) : IEnumerator<TableValueHolder>
            {
                private TreeIterator _it;
                private SeekState _state;

                public TableValueHolder Current { get; private set; }

                object IEnumerator.Current => Current;

                public bool MoveNext()
                {
                    switch (_state)
                    {
                        case SeekState.NotStarted:
                            _state = SeekState.Running;

                            var tree = table.GetTree(table._schema.Key);
                            if (tree == null)
                                return Finish();

                            _it = tree.Iterate(table._prefetch);
                            if (_it.Seek(value) == false || _it.Skip(backward ? -skip : skip) == false)
                                return Finish();

                            Current = new TableValueHolder();
                            break;

                        case SeekState.Running:
                            if ((backward ? _it.MovePrev() : _it.MoveNext()) == false)
                                return Finish();
                            break;

                        default:
                            return false;
                    }

                    table.GetTableValueReader(_it, out Current.Reader);
                    return true;
                }

                private bool Finish()
                {
                    Dispose();
                    return false;
                }

                public void Dispose()
                {
                    _state = SeekState.Done;
                    _it?.Dispose();
                    _it = null;
                }

                public void Reset() => throw new NotSupportedException();
            }
        }

        public readonly struct FixedSizeIndexSeek(Table table, TableSchema.FixedSizeKeyIndexDef index, long key, long skip, bool backward) : IEnumerable<TableValueHolder>
        {
            public Enumerator GetEnumerator() => new(table, index, key, skip, backward);

            IEnumerator<TableValueHolder> IEnumerable<TableValueHolder>.GetEnumerator() => GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public struct Enumerator(Table table, TableSchema.FixedSizeKeyIndexDef index, long key, long skip, bool backward) : IEnumerator<TableValueHolder>
            {
                private FixedSizeTree.IFixedSizeIterator _it;
                private SeekState _state;

                public TableValueHolder Current { get; private set; }

                object IEnumerator.Current => Current;

                public bool MoveNext()
                {
                    switch (_state)
                    {
                        case SeekState.NotStarted:
                            _state = SeekState.Running;

                            _it = table.GetFixedSizeTree(index).Iterate(table._prefetch);
                            if ((backward ? _it.SeekBackward(key) : _it.Seek(key)) == false ||
                                _it.Skip(backward ? -skip : skip) == false)
                                return Finish();

                            Current = new TableValueHolder();
                            break;

                        case SeekState.Running:
                            if (Advance() == false)
                                return Finish();
                            break;

                        default:
                            return false;
                    }

                    while (TryReadCurrent() == false)
                    {
                        if (Advance() == false)
                            return Finish();
                    }

                    return true;
                }

                private bool Advance() => backward ? _it.MovePrev() : _it.MoveNext();

                // going forward, a value that can't be read is reported to the corrupted data handler and skipped
                private bool TryReadCurrent()
                {
                    if (backward || table._onCorruptedDataHandler == null)
                    {
                        table.GetTableValueReader(_it, out Current.Reader);
                        return true;
                    }

                    try
                    {
                        table.GetTableValueReader(_it, out Current.Reader);
                        return true;
                    }
                    catch (InvalidOperationException e)
                    {
                        table._onCorruptedDataHandler.Invoke(table, e);
                        return false;
                    }
                }

                private bool Finish()
                {
                    Dispose();
                    return false;
                }

                public void Dispose()
                {
                    _state = SeekState.Done;
                    _it?.Dispose();
                    _it = null;
                }

                public void Reset() => throw new NotSupportedException();
            }
        }

        public readonly struct IndexSeek(Table table, TableSchema.AbstractTreeIndexDef index, Slice start, Slice prefix, long skip, bool backward) : IEnumerable<SeekResult>
        {
            public Enumerator GetEnumerator() => new(table, index, start, prefix, skip, backward);

            IEnumerator<SeekResult> IEnumerable<SeekResult>.GetEnumerator() => GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            // Goes over the index values (keys of the index tree) and, for each value, over the ids in its fixed size tree.
            public struct Enumerator(Table table, TableSchema.AbstractTreeIndexDef index, Slice start, Slice prefix, long skip, bool backward) : IEnumerator<SeekResult>
            {
                private Tree _tree;
                private TreeIterator _it;
                private FixedSizeTree.IFixedSizeIterator _idsIt; // null when the value has a single id
                private bool _valueOpened;
                private bool _idIsKeyValue;
                private TableValueHolder _result;
                private SeekState _state;

                public SeekResult Current { get; private set; }

                object IEnumerator.Current => Current;

                public bool MoveNext()
                {
                    switch (_state)
                    {
                        case SeekState.NotStarted:
                            _state = SeekState.Running;

                            _tree = table.GetTree(index);
                            if (_tree == null)
                                return Finish();

                            _idIsKeyValue = index is TableSchema.DynamicKeyIndexDef { SupportDuplicateKeys: false };
                            _it = _tree.Iterate(table._prefetch);
                            if (SeekFirstValue() == false)
                                return Finish();
                            break;

                        case SeekState.Running:
                            break;

                        default:
                            return false;
                    }

                    while (true)
                    {
                        long id;
                        if (_valueOpened == false)
                        {
                            _valueOpened = true;
                            if (OpenValue(out id) == false)
                            {
                                if (NextValue() == false)
                                    return Finish();
                                continue;
                            }
                        }
                        else if (_idsIt == null || (backward ? _idsIt.MovePrev() : _idsIt.MoveNext()) == false)
                        {
                            if (NextValue() == false)
                                return Finish();
                            continue;
                        }
                        else
                        {
                            id = _idsIt.CurrentKey;
                        }

                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }

                        _result ??= new TableValueHolder();
                        table.ReadById(id, out _result.Reader);
                        Current = new SeekResult { Key = _it.CurrentKey, Result = _result };
                        return true;
                    }
                }

                private bool SeekFirstValue()
                {
                    if (backward == false)
                    {
                        if (prefix.HasValue)
                            _it.SetRequiredPrefix(prefix);
                        return _it.Seek(start);
                    }

                    // the prefix is set after the seek, since the value at or before start may be past the end of the prefix,
                    // with the last value of the prefix just before it
                    if (_it.SeekBackward(start) == false)
                        return false;

                    if (prefix.HasValue == false)
                        return true;

                    _it.SetRequiredPrefix(prefix);
                    return SliceComparer.StartWith(_it.CurrentKey, _it.RequiredPrefix) || _it.MovePrev();
                }

                private bool OpenValue(out long id)
                {
                    if (_idIsKeyValue)
                    {
                        id = _it.CreateReaderForCurrent().ReadLittleEndianInt64();
                        return true;
                    }

                    var ids = table.GetFixedSizeTreeForValue(_tree, _it.CurrentKey, index.IsGlobal);
                    if (ids.NumberOfEntries == 1) // the common case of a unique value, without an iterator
                        return backward ? ids.TryGetLastKey(out id) : ids.TryGetFirstKey(out id);

                    _idsIt = ids.Iterate(backward && table._prefetch);
                    if ((backward ? _idsIt.SeekToLast() : _idsIt.Seek(long.MinValue)) == false)
                    {
                        id = 0;
                        return false;
                    }

                    id = _idsIt.CurrentKey;
                    return true;
                }

                private bool NextValue()
                {
                    _idsIt?.Dispose();
                    _idsIt = null;
                    _valueOpened = false;
                    return backward ? _it.MovePrev() : _it.MoveNext();
                }

                private bool Finish()
                {
                    Dispose();
                    return false;
                }

                public void Dispose()
                {
                    _state = SeekState.Done;
                    _idsIt?.Dispose();
                    _idsIt = null;
                    _it?.Dispose();
                    _it = null;
                }

                public void Reset() => throw new NotSupportedException();
            }
        }
    }
}
