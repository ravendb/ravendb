using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Lucene.Net.Support;
using Raven.Server.Documents.Replication;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public enum ConflictStatus
    {
        Update,
        Conflict,
        AlreadyMerged
    }

    public static class ChangeVectorUtils
    {
        public static ConflictStatus GetConflictStatus(string remoteAsString, string localAsString)
        {
            if (remoteAsString == localAsString)
                return ConflictStatus.AlreadyMerged;

            if (string.IsNullOrEmpty(remoteAsString))
                return ConflictStatus.AlreadyMerged;

            if (string.IsNullOrEmpty(localAsString))
                return ConflictStatus.Update;

            var local = localAsString.ToChangeVector();
            var remote = remoteAsString.ToChangeVector();

            //any missing entries from a change vector are assumed to have zero value
            var localHasLargerEntries = false;
            var remoteHasLargerEntries = false;

            int numOfMatches = 0;
            for (int i = 0; i < remote.Length; i++)
            {
                bool found = false;
                for (int j = 0; j < local.Length; j++)
                {
                    if (remote[i].DbId == local[j].DbId)
                    {
                        found = true;
                        numOfMatches++;

                        if (remote[i].Etag > local[j].Etag)
                        {
                            remoteHasLargerEntries = true;
                        }
                        else if (remote[i].Etag < local[j].Etag)
                        {
                            localHasLargerEntries = true;
                        }
                        break;
                    }
                }
                if (found == false)
                {
                    remoteHasLargerEntries = true;
                }
            }
            if (numOfMatches < local.Length)
            {
                localHasLargerEntries = true;
            }

            if (remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.Conflict;

            if (remoteHasLargerEntries == false && localHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical

            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;
        }

        [ThreadStatic] private static string _dbIdBuffer;

        [ThreadStatic] private static StringBuilder _changeVectorBuffer;

        private static int NumberOfDigits(long etag)
        {
            int count = 0;
            do
            {
                count++;
                etag /= 10;
            } while (etag != 0);
            return count;
        }

        private static void WriteNumberBackwards(StringBuilder sb, int offset, long etag)
        {
            do
            {
                var rem = etag % 10;
                etag /= 10;
                sb[offset--] = (char)((char)rem + '0');
            } while (etag != 0);
        }

        private static long ParseToLong(string s, int start, int len)
        {
            int num;
            num = s[start] - '0';
            for (int i = 1; i < len; i++)
            {
                num *= 10;
                num += s[start + i] - '0';
            }
            return num;
        }

        public static unsafe bool TryUpdateChangeVector(string nodeTag, Guid dbId, long etag, ref string changeVector)
        {
            InitiailizeThreadLocalState();

            Debug.Assert(changeVector != null);


            fixed (char* pChars = _dbIdBuffer)
            {
                var result = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&dbId, 0, 16);
                Debug.Assert(result == 22);
            }
            var newEtagLen = NumberOfDigits(etag);
            var dbIndex = changeVector.IndexOf(_dbIdBuffer, StringComparison.Ordinal);

            if (dbIndex < 0)
            {
                if (string.IsNullOrEmpty(changeVector) == false)
                {
                    _changeVectorBuffer.Append(changeVector)
                        .Append(", ");
                }

                _changeVectorBuffer.Append(nodeTag)
                    .Append(':')
                    .Append(etag)
                    .Append('-')
                    .Append(_dbIdBuffer);

                changeVector = _changeVectorBuffer.ToString();
                return true;
            }

            var existingEtagEndIndex = dbIndex - 1;
            var currentEtagStartIndex = changeVector.LastIndexOf(':', existingEtagEndIndex) + 1;

            var existingLen = existingEtagEndIndex - currentEtagStartIndex;
            var existingEtag = ParseToLong(changeVector, currentEtagStartIndex, existingLen);
            // assume no trailing zeros
            var diff = newEtagLen - existingLen;
            if (diff == 0)
            {
                // compare the strings instead of parsing to int
                if (existingEtag >= etag)
                {
                    //nothing to do
                    return false;
                }
                // we clone the string because others might hold a reference to it and consider it immutable
                _changeVectorBuffer.Append(changeVector);

                // replace the etag

                WriteNumberBackwards(_changeVectorBuffer, currentEtagStartIndex + newEtagLen - 1, etag);
                changeVector = _changeVectorBuffer.ToString();
                return true;
            }
            if (diff < 0)
            {
                // nothing to do, already known to be smaller
                return false;
            }
            // allocate new string
            _changeVectorBuffer.Append(changeVector, 0, currentEtagStartIndex)
                .Append(etag)
                .Append(changeVector, existingEtagEndIndex, changeVector.Length - existingEtagEndIndex);
            changeVector = _changeVectorBuffer.ToString();
            return true;
        }

        private static void InitiailizeThreadLocalState()
        {
            if (_dbIdBuffer == null)
                _dbIdBuffer = new string(' ', 22);
            if (_changeVectorBuffer == null)
                _changeVectorBuffer = new StringBuilder();
            _changeVectorBuffer.Length = 0;
        }

        [ThreadStatic] private static List<ChangeVectorEntry> _mergeVectorBuffer;

        public static string MergeVectors(string vectorAstring, string vectorBstring)
        {
            if (string.IsNullOrEmpty(vectorAstring))
                return vectorBstring;
            if (string.IsNullOrEmpty(vectorBstring))
                return vectorAstring;

            if (_mergeVectorBuffer == null)
                _mergeVectorBuffer = new EquatableList<ChangeVectorEntry>();
            _mergeVectorBuffer.Clear();

            ChangeVectorParser.MergeChangeVector(vectorAstring, _mergeVectorBuffer);
            ChangeVectorParser.MergeChangeVector(vectorBstring, _mergeVectorBuffer);

            return _mergeVectorBuffer.SerializeVector();
        }

        public static string MergeVectors(List<string> changeVectors)
        {
            if (_mergeVectorBuffer == null)
                _mergeVectorBuffer = new EquatableList<ChangeVectorEntry>();
            _mergeVectorBuffer.Clear();

            for (int i = 0; i < changeVectors.Count; i++)
            {
                ChangeVectorParser.MergeChangeVector(changeVectors[i], _mergeVectorBuffer);
            }

            return _mergeVectorBuffer.SerializeVector();
        }

        public static unsafe string NewChangeVector(string nodeTag, long etag, Guid dbId)
        {
            InitiailizeThreadLocalState();

            fixed (char* pChars = _dbIdBuffer)
            {
                var result = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&dbId, 0, 16);
                Debug.Assert(result == 22);
            }

            return _changeVectorBuffer
                .Append(nodeTag)
                .Append(':')
                .Append(etag)
                .Append('-')
                .Append(_dbIdBuffer)
                .ToString();
        }
    }
}
