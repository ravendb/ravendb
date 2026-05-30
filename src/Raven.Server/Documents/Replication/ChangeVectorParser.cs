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

        public static readonly int RaftInt = ParseNodeTag(RaftTag.AsSpan(), 0, RaftTag.Length - 1);
        public static readonly int TrxnInt = ParseNodeTag(TrxnTag.AsSpan(), 0, TrxnTag.Length - 1);
        public static readonly int SinkInt = ParseNodeTag(SinkTag.AsSpan(), 0, SinkTag.Length - 1);
        public static readonly int MoveInt = ParseNodeTag(MoveTag.AsSpan(), 0, MoveTag.Length - 1);
        public static readonly int DbBase64IdSize = 23;

        private enum State
        {
            Tag,
            Etag,
            Whitespace
        }

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
            var start = 0;
            var current = 0;
            var state = State.Tag;
            int tag = -1;

            while (current < changeVector.Length)
            {
                switch (state)
                {
                    case State.Tag:
                        if (changeVector[current] == ':')
                        {
                            tag = ParseNodeTag(changeVector.AsSpan(), start, current - 1);
                            state = State.Etag;
                            start = current + 1;
                        }
                        current++;
                        break;
                    case State.Etag:
                        if (changeVector[current] == '-')
                        {
                            var etag = ParseEtag(changeVector.AsSpan(), start, current - 1);
                            if (current + DbBase64IdSize > changeVector.Length)
                                ThrowInvalidEndOfString("DbId", changeVector);
                            list.Add(new ChangeVectorEntry
                            {
                                NodeTag = tag,
                                Etag = etag,
                                DbId = changeVector.Substring(current + 1, 22)
                            });
                            start = current + DbBase64IdSize;
                            current = start;
                            state = State.Whitespace;
                        }
                        current++;
                        break;
                    case State.Whitespace:
                        if (char.IsWhiteSpace(changeVector[current]) ||
                            changeVector[current] == ',')
                        {
                            start++;
                            current++;
                        }
                        else
                        {
                            start = current;
                            current++;
                            state = State.Tag;
                        }
                        break;

                    default:
                        ThrowInvalidState(state, changeVector);
                        break;
                }
            }

            if (state == State.Whitespace)
                return list;

            ThrowInvalidEndOfString(state.ToString(), changeVector);
            return null; // never hit
        }

        public static ChangeVectorEntry[] ToChangeVector(this string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return Array.Empty<ChangeVectorEntry>();

            return changeVector.ToChangeVectorList().ToArray();
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
        private static void ThrowInvalidEndOfString(string state, string cv)
        {
            throw new ArgumentException("Expected " + state + ", but got end of string in : " + cv);
        }

        [DoesNotReturn]
        private static void ThrowInvalidState(State state, string cv)
        {
            throw new ArgumentOutOfRangeException(state + " in " + cv);
        }
    }
}
