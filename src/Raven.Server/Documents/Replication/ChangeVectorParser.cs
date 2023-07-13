using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorParser
    {
        public const string RaftTag = "RAFT";
        public const string TrxnTag = "TRXN";
        public const string SinkTag = "SINK";
        public const string MoveTag = "MOVE";

        public static readonly int RaftInt = RaftTag.ParseNodeTag();
        public static readonly int TrxnInt = TrxnTag.ParseNodeTag();
        public static readonly int SinkInt = SinkTag.ParseNodeTag();
        public static readonly int MoveInt = MoveTag.ParseNodeTag();
        public static readonly int DbBase64IdSize = 23;

        private enum State
        {
            Tag,
            Etag,
            Whitespace
        }
         
        public static long GetEtagByNode(string changeVector, string nodeTag)
        {
            int nodeStart = 0;
            while (nodeStart + nodeTag.Length < changeVector.Length ) 
            {
                var endNode = changeVector.IndexOf(':', nodeStart);
                if (string.Compare(nodeTag, 0, changeVector, nodeStart, endNode - nodeStart) == 0)
                {
                    long etagVal = 0;
                    var endEtag = changeVector.IndexOf('-', endNode + 1);
                    for (int i = endNode+1; i < endEtag; i++)
                    {
                        etagVal *= 10;
                        etagVal += changeVector[i] - '0';
                    }
                    return etagVal;
                }
                nodeStart = changeVector.IndexOf(' ', endNode);
                if (nodeStart == -1)
                    break;
                nodeStart++;
            }

            return 0;
        }
       

        public static int ParseNodeTag(this string nodeTag)
        {
            return ParseNodeTag(nodeTag, 0, nodeTag.Length - 1);
        }

        public static int ParseNodeTag(string changeVector, int start, int end)
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

        private static long ParseEtag(string changeVector, int start, int end)
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
                            tag = ParseNodeTag(changeVector, start, current - 1);
                            state = State.Etag;
                            start = current + 1;
                        }
                        current++;
                        break;
                    case State.Etag:
                        if (changeVector[current] == '-')
                        {
                            var etag = ParseEtag(changeVector, start, current - 1);
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

        public static void MergeChangeVector(string changeVector, List<ChangeVectorEntry> entries)
        {
            if (string.IsNullOrEmpty(changeVector))
                return;

            AssertChangeVector(changeVector);

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
                            tag = ParseNodeTag(changeVector, start, current - 1);
                            state = State.Etag;
                            start = current + 1;
                        }
                        current++;
                        break;
                    case State.Etag:
                        if (changeVector[current] == '-')
                        {
                            var etag = ParseEtag(changeVector, start, current - 1);

                            if (current + DbBase64IdSize > changeVector.Length)
                                ThrowInvalidEndOfString("DbId", changeVector);
                            bool found = false;
                            var dbId = changeVector.Substring(current + 1, 22);
                            for (int i = 0; i < entries.Count; i++)
                            {
                                if (entries[i].DbId == dbId)
                                {
                                    if (entries[i].Etag < etag)
                                    {
                                        entries[i] = new ChangeVectorEntry
                                        {
                                            NodeTag = tag,
                                            Etag = etag,
                                            DbId = dbId
                                        };
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            if (found == false)
                            {
                                entries.Add(new ChangeVectorEntry
                                {
                                    NodeTag = tag,
                                    Etag = etag,
                                    DbId = dbId
                                });
                            }
                          
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
                return;

            ThrowInvalidEndOfString(state.ToString(), changeVector);
        }

        public static bool MergeChangeVectorDown(string changeVector, List<ChangeVectorEntry> entries)
        {
            var start = 0;
            var current = 0;
            var state = State.Tag;
            int tag = -1;
            bool found = false;

            while (current < changeVector.Length)
            {
                switch (state)
                {
                    case State.Tag:
                        if (changeVector[current] == ':')
                        {
                            tag = ParseNodeTag(changeVector, start, current - 1);
                            state = State.Etag;
                            start = current + 1;
                        }
                        current++;
                        break;
                    case State.Etag:
                        if (changeVector[current] == '-')
                        {
                            var etag = ParseEtag(changeVector, start, current - 1);

                            if (current + DbBase64IdSize > changeVector.Length)
                                ThrowInvalidEndOfString("DbId", changeVector);
                 
                            var dbId = changeVector.Substring(current + 1, 22);
                            for (int i = 0; i < entries.Count; i++)
                            {
                                if (entries[i].DbId == dbId)
                                {
                                    found = true;
                                    if (entries[i].Etag > etag)
                                    {
                                        entries[i] = new ChangeVectorEntry
                                        {
                                            NodeTag = tag,
                                            Etag = etag,
                                            DbId = dbId
                                        };
                                    }
                                    break;
                                }
                            }
                          
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
                return found;

            ThrowInvalidEndOfString(state.ToString(), changeVector);
            return false;
        }

        [Conditional("DEBUG")]
        public static void AssertChangeVector(string changeVector)
        {
            if (changeVector.Contains('|'))
                Debug.Assert(false, $"Cannot contain pipe {changeVector}");
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
