using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorParser
    {
        private enum State
        {
            Tag,
            Etag,
            Whitespace,
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

        private static void ThrowInvalidNodeTag(char ch)
        {
            throw new ArgumentException("Invalid node tag character: " + ch);
        }

        private static long ParseEtag(string changeVector, int start, int end)
        {
            int etag = changeVector[start] - '0';

            for (int i = start + 1; i <= end; i++)
            {
                etag *= 10;
                etag += changeVector[i] - '0';
            }
            return etag;
        }

        private static unsafe Guid ParseDbId(string changeVector, int start)
        {
            Guid id = Guid.Empty;
            char* buffer = stackalloc char[24];
            fixed (char* str = changeVector)
            {
                Buffer.MemoryCopy(str + start, buffer, 24 * sizeof(char), 22 * sizeof(char));
                buffer[22] = '=';
                buffer[23] = '=';
                FromBase64_Decode(buffer, 24, (byte*)&id, 16);
            }
            return id;
        }


        // This is taken from: https://github.com/dotnet/coreclr/blob/9870e4edc898d24fb5d066b08d1472a03e4c75c5/src/mscorlib/shared/System/Convert.cs#L2794
        // this is an internal method, and the cost of delegate indirection in calling it was significant enough to cause us to inline it
        private static unsafe Int32 FromBase64_Decode(Char* startInputPtr, Int32 inputLength, Byte* startDestPtr, Int32 destLength)
        {

            // You may find this method weird to look at. It’s written for performance, not aesthetics.
            // You will find unrolled loops label jumps and bit manipulations.

            const UInt32 intA = (UInt32)'A';
            const UInt32 inta = (UInt32)'a';
            const UInt32 int0 = (UInt32)'0';
            const UInt32 intEq = (UInt32)'=';
            const UInt32 intPlus = (UInt32)'+';
            const UInt32 intSlash = (UInt32)'/';
            const UInt32 intSpace = (UInt32)' ';
            const UInt32 intTab = (UInt32)'\t';
            const UInt32 intNLn = (UInt32)'\n';
            const UInt32 intCRt = (UInt32)'\r';
            const UInt32 intAtoZ = (UInt32)('Z' - 'A');  // = ('z' - 'a')
            const UInt32 int0to9 = (UInt32)('9' - '0');

            Char* inputPtr = startInputPtr;
            Byte* destPtr = startDestPtr;

            // Pointers to the end of input and output:
            Char* endInputPtr = inputPtr + inputLength;
            Byte* endDestPtr = destPtr + destLength;

            // Current char code/value:
            UInt32 currCode;

            // This 4-byte integer will contain the 4 codes of the current 4-char group.
            // Eeach char codes for 6 bits = 24 bits.
            // The remaining byte will be FF, we use it as a marker when 4 chars have been processed.            
            UInt32 currBlockCodes = 0x000000FFu;

            unchecked
            {
                while (true)
                {

                    // break when done:
                    if (inputPtr >= endInputPtr)
                        goto _AllInputConsumed;

                    // Get current char:
                    currCode = (UInt32)(*inputPtr);
                    inputPtr++;

                    // Determine current char code:

                    if (currCode - intA <= intAtoZ)
                        currCode -= intA;

                    else if (currCode - inta <= intAtoZ)
                        currCode -= (inta - 26u);

                    else if (currCode - int0 <= int0to9)
                        currCode -= (int0 - 52u);

                    else
                    {
                        // Use the slower switch for less common cases:
                        switch (currCode)
                        {

                            // Significant chars:
                            case intPlus:
                                currCode = 62u;
                                break;

                            case intSlash:
                                currCode = 63u;
                                break;

                            // Legal no-value chars (we ignore these):
                            case intCRt:
                            case intNLn:
                            case intSpace:
                            case intTab:
                                continue;

                            // The equality char is only legal at the end of the input.
                            // Jump after the loop to make it easier for the JIT register predictor to do a good job for the loop itself:
                            case intEq:
                                goto _EqualityCharEncountered;

                            // Other chars are illegal:
                            default:
                                throw new FormatException(("Format_BadBase64Char"));
                        }
                    }

                    // Ok, we got the code. Save it:
                    currBlockCodes = (currBlockCodes << 6) | currCode;

                    // Last bit in currBlockCodes will be on after in shifted right 4 times:
                    if ((currBlockCodes & 0x80000000u) != 0u)
                    {

                        if ((Int32)(endDestPtr - destPtr) < 3)
                            return -1;

                        *(destPtr) = (Byte)(currBlockCodes >> 16);
                        *(destPtr + 1) = (Byte)(currBlockCodes >> 8);
                        *(destPtr + 2) = (Byte)(currBlockCodes);
                        destPtr += 3;

                        currBlockCodes = 0x000000FFu;
                    }

                }
            }  // unchecked while

        // 'd be nice to have an assert that we never get here, but CS0162: Unreachable code detected.
        // Contract.Assert(false, "We only leave the above loop by jumping; should never get here.");

        // We jump here out of the loop if we hit an '=':
        _EqualityCharEncountered:

            // Recall that inputPtr is now one position past where '=' was read.
            // '=' can only be at the last input pos:
            if (inputPtr == endInputPtr)
            {

                // Code is zero for trailing '=':
                currBlockCodes <<= 6;

                // The '=' did not complete a 4-group. The input must be bad:
                if ((currBlockCodes & 0x80000000u) == 0u)
                    throw new FormatException(("Format_BadBase64CharArrayLength"));

                if ((int)(endDestPtr - destPtr) < 2)  // Autch! We underestimated the output length!
                    return -1;

                // We are good, store bytes form this past group. We had a single "=", so we take two bytes:
                *(destPtr++) = (Byte)(currBlockCodes >> 16);
                *(destPtr++) = (Byte)(currBlockCodes >> 8);

                currBlockCodes = 0x000000FFu;

            }
            else
            { // '=' can also be at the pre-last position iff the last is also a '=' excluding the white spaces:

                // We need to get rid of any intermediate white spaces.
                // Otherwise we would be rejecting input such as "abc= =":
                while (inputPtr < (endInputPtr - 1))
                {
                    Int32 lastChar = *(inputPtr);
                    if (lastChar != (Int32)' ' && lastChar != (Int32)'\n' && lastChar != (Int32)'\r' && lastChar != (Int32)'\t')
                        break;
                    inputPtr++;
                }

                if (inputPtr == (endInputPtr - 1) && *(inputPtr) == '=')
                {

                    // Code is zero for each of the two '=':
                    currBlockCodes <<= 12;

                    // The '=' did not complete a 4-group. The input must be bad:
                    if ((currBlockCodes & 0x80000000u) == 0u)
                        throw new FormatException(("Format_BadBase64CharArrayLength"));

                    if ((Int32)(endDestPtr - destPtr) < 1)  // Autch! We underestimated the output length!
                        return -1;

                    // We are good, store bytes form this past group. We had a "==", so we take only one byte:
                    *(destPtr++) = (Byte)(currBlockCodes >> 16);

                    currBlockCodes = 0x000000FFu;

                }
                else  // '=' is not ok at places other than the end:
                    throw new FormatException(("Format_BadBase64Char"));

            }

        // We get here either from above or by jumping out of the loop:
        _AllInputConsumed:

            // The last block of chars has less than 4 items
            if (currBlockCodes != 0x000000FFu)
                throw new FormatException(("Format_BadBase64CharArrayLength"));

            // Return how many bytes were actually recovered:
            return (Int32)(destPtr - startDestPtr);

        } // Int32 FromBase64_Decode(...)

        public static ChangeVectorEntry[] ToChangeVector(this string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return new ChangeVectorEntry[0];

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
                            if (current + 23 > changeVector.Length)
                                ThrowInvalidEndOfString("DbId", changeVector);
                            list.Add(new ChangeVectorEntry
                            {
                                NodeTag = tag,
                                Etag = etag,
                                DbId = ParseDbId(changeVector, current + 1),
                            });
                            start = current + 23;
                            current = start;
                            state = State.Whitespace;
                        }
                        current++;
                        break;
                    case State.Whitespace:
                        if (char.IsWhiteSpace(changeVector[current]) ||
                            changeVector[current] == '=' || // TODO: Remove me
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
                return list.ToArray();

            ThrowInvalidEndOfString(state.ToString(), changeVector);
            return default(ChangeVectorEntry[]); // never hit
        }


        public static void MergeChangeVector(string changeVector, List<ChangeVectorEntry> entries)
        {
            if (string.IsNullOrEmpty(changeVector))
                return;

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
                            if (current + 23 > changeVector.Length)
                                ThrowInvalidEndOfString("DbId", changeVector);
                            bool found = false;
                            var dbId = ParseDbId(changeVector, current + 1);
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
                          
                            start = current + 23;
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

        private static void ThrowInvalidEndOfString(string state, string cv)
        {
            throw new ArgumentException("Expected " + state + ", but got end of string in : " + cv);
        }

        private static void ThrowInvalidState(State state, string cv)
        {
            throw new ArgumentOutOfRangeException(state.ToString() + " in " + cv);
        }
    }
}
