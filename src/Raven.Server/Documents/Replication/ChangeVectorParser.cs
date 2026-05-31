using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorParser
    {
        public const string RaftTag = "RAFT";
        public const string TrxnTag = "TRXN";
        public const string SinkTag = "SINK";
        public const string MoveTag = "MOVE";
        public const int DbBase64IdSize = 23;

        public static readonly int RaftInt = ParseNodeTag(RaftTag.AsSpan(), 0, RaftTag.Length - 1);
        public static readonly int TrxnInt = ParseNodeTag(TrxnTag.AsSpan(), 0, TrxnTag.Length - 1);
        public static readonly int SinkInt = ParseNodeTag(SinkTag.AsSpan(), 0, SinkTag.Length - 1);
        public static readonly int MoveInt = ParseNodeTag(MoveTag.AsSpan(), 0, MoveTag.Length - 1);

        public static int ParseNodeTag(ReadOnlySpan<char> changeVector, int start, int end)
        {
            AssertValidNodeTagChar(changeVector[end]);

            int tag = changeVector[end] - 'A';

            for (int i = end - 1; i >= start; i--)
            {
                AssertValidNodeTagChar(changeVector[i]);
                tag *= 26;
                tag += changeVector[i] - 'A';
            }
            return tag;
        }

        private static void AssertValidNodeTagChar(char ch)
        {
            if (ch < 'A' || ch > 'Z')
                ThrowInvalidNodeTag(ch);
        }

        [DoesNotReturn]
        private static void ThrowInvalidNodeTag(char ch)
        {
            throw new ArgumentException("Invalid node tag character: " + ch);
        }

        internal static long ParseEtag(ReadOnlySpan<char> changeVector, int start, int end)
        {
            long etag = changeVector[start] - '0';

            for (int i = start + 1; i <= end; i++)
            {
                etag *= 10;
                etag += changeVector[i] - '0';
            }
            return etag;
        }

        public static List<ChangeVectorEntry> ToChangeVectorList(this string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return null;

            var list = new List<ChangeVectorEntry>();
            var enumerator = new ChangeVectorEnumerator(changeVector.AsSpan());
            while (enumerator.MoveNext())
            {
                list.Add(new ChangeVectorEntry
                {
                    NodeTag = enumerator.NodeTag,
                    Etag = enumerator.Etag,
                    DbId = enumerator.DbId.ToString()
                });
            }

            return list;
        }

        public static ChangeVectorEntry[] ToChangeVector(this string changeVector)
        {
            return string.IsNullOrEmpty(changeVector)
                ? []
                : changeVector.ToChangeVectorList().ToArray();
        }

        [Conditional("DEBUG")]
        public static void AssertChangeVector(string changeVector)
        {
            if (changeVector.Contains('|'))
                Debug.Assert(false, $"Cannot contain pipe {changeVector}");
        }

        [Conditional("DEBUG")]
        public static void AssertChangeVector(ReadOnlySpan<char> changeVector)
        {
            if (changeVector.IndexOf('|') >= 0)
                Debug.Assert(false, $"Cannot contain pipe {changeVector.ToString()}");
        }

        [DoesNotReturn]
        internal static void ThrowInvalidEndOfString(string state, ReadOnlySpan<char> cv)
        {
            throw new ArgumentException($"Expected '{state}' but got end of string in: '{cv}'");
        }

        [DoesNotReturn]
        internal static void ThrowInvalidState<TState>(TState state, ReadOnlySpan<char> cv)
        {
            throw new ArgumentOutOfRangeException(nameof(state), state,
                $"Unexpected change vector parser state '{state}' while parsing: '{cv}'");
        }
    }

    internal ref struct ChangeVectorEnumerator
    {
        private enum State
        {
            Tag,
            Etag,
            Whitespace
        }

        private readonly ReadOnlySpan<char> _changeVector;
        private int _start;
        private int _current;
        private State _state;
        private int _tag;

        public ChangeVectorEnumerator(ReadOnlySpan<char> changeVector)
        {
            _changeVector = changeVector;
            _start = 0;
            _current = 0;
            _state = State.Tag;
            _tag = -1;
            NodeTag = -1;
            Etag = 0;
            DbIdStart = -1;
            DbIdLength = 0;
        }

        public int NodeTag { get; private set; }

        public long Etag { get; private set; }

        public ReadOnlySpan<char> DbId => _changeVector.Slice(DbIdStart, DbIdLength);

        public int DbIdStart { get; private set; }

        public int DbIdLength { get; private set; }

        public bool MoveNext()
        {
            if (_changeVector.Length == 0)
                return false;

            while (_current < _changeVector.Length)
            {
                switch (_state)
                {
                    case State.Tag:
                        if (_changeVector[_current] == ':')
                        {
                            _tag = ChangeVectorParser.ParseNodeTag(_changeVector, _start, _current - 1);
                            _state = State.Etag;
                            _start = _current + 1;
                        }
                        _current++;
                        break;

                    case State.Etag:
                        if (_changeVector[_current] == '-')
                        {
                            var etag = ChangeVectorParser.ParseEtag(_changeVector, _start, _current - 1);
                            if (_current + ChangeVectorParser.DbBase64IdSize > _changeVector.Length)
                                ChangeVectorParser.ThrowInvalidEndOfString("DbId", _changeVector);

                            NodeTag = _tag;
                            Etag = etag;
                            DbIdStart = _current + 1;
                            DbIdLength = 22;

                            _start = _current + ChangeVectorParser.DbBase64IdSize;
                            _current = _start + 1;
                            _state = State.Whitespace;
                            return true;
                        }
                        _current++;
                        break;

                    case State.Whitespace:
                        if (char.IsWhiteSpace(_changeVector[_current]) ||
                            _changeVector[_current] == ',')
                        {
                            _start++;
                            _current++;
                        }
                        else
                        {
                            _start = _current;
                            _current++;
                            _state = State.Tag;
                        }
                        break;

                    default:
                        ChangeVectorParser.ThrowInvalidState(_state, _changeVector);
                        break;
                }
            }

            if (_state == State.Whitespace)
                return false;

            ChangeVectorParser.ThrowInvalidEndOfString(_state.ToString(), _changeVector);
            return false;
        }
    }
}
