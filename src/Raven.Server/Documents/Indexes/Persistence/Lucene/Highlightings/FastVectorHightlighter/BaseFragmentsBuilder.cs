/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Text;

using Lucene.Net.Documents;
using Lucene.Net.Index;

using WeightedFragInfo = Lucene.Net.Search.Vectorhighlight.FieldFragList.WeightedFragInfo;
using System.Linq;
using Lucene.Net.Store;

namespace Lucene.Net.Search.Vectorhighlight
{
    public abstract class BaseFragmentsBuilder : FragmentsBuilder
    {
        protected String[] preTags, postTags;
        public static String[] COLORED_PRE_TAGS = {
            "<b style=\"background:yellow\">", "<b style=\"background:lawngreen\">", "<b style=\"background:aquamarine\">",
            "<b style=\"background:magenta\">", "<b style=\"background:palegreen\">", "<b style=\"background:coral\">",
            "<b style=\"background:wheat\">", "<b style=\"background:khaki\">", "<b style=\"background:lime\">",
            "<b style=\"background:deepskyblue\">", "<b style=\"background:deeppink\">", "<b style=\"background:salmon\">",
            "<b style=\"background:peachpuff\">", "<b style=\"background:violet\">", "<b style=\"background:mediumpurple\">",
            "<b style=\"background:palegoldenrod\">", "<b style=\"background:darkkhaki\">", "<b style=\"background:springgreen\">",
            "<b style=\"background:turquoise\">", "<b style=\"background:powderblue\">"
        };

        public static String[] COLORED_POST_TAGS = { "</b>" };

        protected BaseFragmentsBuilder()
            : this(new String[] { "<b>" }, new String[] { "</b>" })
        {

        }

        protected BaseFragmentsBuilder(String[] preTags, String[] postTags)
        {
            this.preTags = preTags;
            this.postTags = postTags;
        }

        static Object CheckTagsArgument(Object tags)
        {
            if (tags is String)
                return tags;
            else if (tags is String[])
                return tags;
            throw new ArgumentException("type of preTags/postTags must be a String or String[]");
        }

        public abstract List<WeightedFragInfo> GetWeightedFragInfoList(List<WeightedFragInfo> src);

        public virtual string CreateFragment(IndexReader reader, int docId, string fieldName, FieldFragList fieldFragList, int fragCharSize, IState state)
        {
            String[] fragments = CreateFragments(reader, docId, fieldName, fieldFragList, 1, fragCharSize, state);
            if (fragments == null || fragments.Length == 0)
                return null;
            return fragments[0];
        }

        public virtual string[] CreateFragments(IndexReader reader, int docId, string fieldName, FieldFragList fieldFragList, int maxNumFragments, int fragCharSize, IState state)
        {
            if (maxNumFragments < 0)
                throw new ArgumentException("maxNumFragments(" + maxNumFragments + ") must be positive number.");

            List<WeightedFragInfo> fragInfos = GetWeightedFragInfoList(fieldFragList.fragInfos);

            List<String> fragments = new List<String>(maxNumFragments);
            Field[] values = GetFields(reader, docId, fieldName, state);
            if (values.Length == 0)
                return null;
            StringBuilder buffer = new StringBuilder();
            int[] nextValueIndex = { 0 };
            for (int n = 0; n < maxNumFragments && n < fragInfos.Count; n++)
            {
                WeightedFragInfo fragInfo = fragInfos[n];
                fragments.Add(MakeFragment(buffer, nextValueIndex, values, fragInfo, fragCharSize, state));
            }
            return fragments.ToArray();
        }

        [Obsolete]
        protected virtual String[] GetFieldValues(IndexReader reader, int docId, String fieldName, IState state)
        {
            Document doc = reader.Document(docId, new MapFieldSelector(new String[] { fieldName }), state);
            return doc.GetValues(fieldName, state); // according to Document class javadoc, this never returns null
        }

        protected virtual Field[] GetFields(IndexReader reader, int docId, String fieldName, IState state)
        {
            // according to javadoc, doc.getFields(fieldName) cannot be used with lazy loaded field???
            Document doc = reader.Document(docId, new MapFieldSelector(new String[] { fieldName }), state);
            return doc.GetFields(fieldName); // according to Document class javadoc, this never returns null
        }

        protected virtual string MakeFragment(StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo, int fragCharSize, IState state)
        {
            int adjustedStartPos;
            var fragmentSource = GetFragmentSource(buffer, index, values, fragInfo, out adjustedStartPos, state);
            return MakeFragment(fragInfo, fragmentSource, adjustedStartPos, fragCharSize);
        }

        private String MakeFragment(WeightedFragInfo fragInfo, String src, int adjustedStart, int fragCharSize)
        {
            StringBuilder fragment = new StringBuilder();
            int srcIndex = 0;
            var items = from subInfo in fragInfo.subInfos
                        from to in subInfo.termsOffsets
                        orderby to.startOffset
                        select new
                        {
                            to,
                            subInfo
                        };
            foreach (var item in items)
            {
                var headerIndex = item.to.startOffset - adjustedStart;
                var matchLen = item.to.endOffset - item.to.startOffset;
                var startLen = Math.Max(0, Math.Min(headerIndex - srcIndex, (fragCharSize - matchLen) / 2));

                fragCharSize -= matchLen + startLen;

                fragment.Append(src.Substring(headerIndex - startLen, startLen))
                    .Append(GetPreTag(item.subInfo.seqnum))
                    .Append(src.Substring(headerIndex, matchLen))
                    .Append(GetPostTag(item.subInfo.seqnum));
                srcIndex = item.to.endOffset - adjustedStart;
            }
            fragment.Append(src.Substring(srcIndex, Math.Min(Math.Max(0, fragCharSize), src.Length - srcIndex)));
            return fragment.ToString();
        }

        /*
        [Obsolete]
        protected String MakeFragment(StringBuilder buffer, int[] index, String[] values, WeightedFragInfo fragInfo)
        {
            StringBuilder fragment = new StringBuilder();
            int s = fragInfo.startOffset;
            String src = GetFragmentSource(buffer, index, values, s, fragInfo.endOffset);
            int srcIndex = 0;
            foreach (SubInfo subInfo in fragInfo.subInfos)
            {
                foreach (Toffs to in subInfo.termsOffsets)
                {
                    fragment.Append(src.Substring(srcIndex, to.startOffset - s - srcIndex)).Append(GetPreTag(subInfo.seqnum))
                      .Append(src.Substring(to.startOffset - s, to.endOffset - s - (to.startOffset - s))).Append(GetPostTag(subInfo.seqnum));
                    srcIndex = to.endOffset - s;
                }
            }
            fragment.Append(src.Substring(srcIndex));
            return fragment.ToString();
        }
        */


        [Obsolete]
        protected virtual String GetFragmentSource(StringBuilder buffer, int[] index, String[] values, int startOffset, int endOffset)
        {
            while (buffer.Length < endOffset && index[0] < values.Length)
            {
                buffer.Append(values[index[0]]);
                if (values[index[0]].Length > 0 && index[0] + 1 < values.Length)
                    buffer.Append(' ');
                index[0]++;
            }
            int eo = buffer.Length < endOffset ? buffer.Length : endOffset;
            return buffer.ToString().Substring(startOffset, eo - startOffset);
        }

        protected virtual String GetFragmentSource(StringBuilder buffer, int[] index, Field[] values, int startOffset, int endOffset, IState state)
        {
            while (buffer.Length < endOffset && index[0] < values.Length)
            {
                buffer.Append(values[index[0]].StringValue(state));
                if (values[index[0]].IsTokenized && values[index[0]].StringValue(state).Length > 0 && index[0] + 1 < values.Length)
                    buffer.Append(' ');
                index[0]++;
            }
            int eo = buffer.Length < endOffset ? buffer.Length : endOffset;

            return buffer.ToString(startOffset, eo - startOffset);
        }

        private string GetFragmentSource(StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo weightedFragInfo, out int startOffset, IState state)
        {
            while (buffer.Length < weightedFragInfo.endOffset && index[0] < values.Length)
            {
                buffer.Append(values[index[0]].StringValue(state));
                if (values[index[0]].IsTokenized && values[index[0]].StringValue(state).Length > 0 && index[0] + 1 < values.Length)
                    buffer.Append(' ');
                index[0]++;
            }

            var endOffset = 0;
            startOffset = buffer.Length - 1;

            foreach (var subInfo in weightedFragInfo.subInfos)
            {
                foreach (var termsOffset in subInfo.termsOffsets)
                {
                    if (termsOffset.startOffset < startOffset)
                        startOffset = termsOffset.startOffset;
                    if (termsOffset.endOffset > endOffset)
                        endOffset = termsOffset.endOffset;
                }
            }

            int maxStart = startOffset;
            int minEnd = endOffset;

            var maxLength = weightedFragInfo.endOffset - weightedFragInfo.startOffset;
            var bufferLength = buffer.Length;
            var stopChars = new[] { ',', '.', ';', '!', '?' };

            while (endOffset - startOffset < maxLength - 2) // limit the size of the returned string
            {
                if (endOffset >= bufferLength) // out of range
                {
                    endOffset = bufferLength;

                    startOffset = Math.Max(0, startOffset - maxLength - endOffset);
                    break;
                }
                if (startOffset == 0) // out of range
                {
                    endOffset = Math.Min(bufferLength, startOffset + maxLength);
                    break;
                }
                var localStartOffset = startOffset;
                if (stopChars.Any(c => c == buffer[localStartOffset]) && buffer[startOffset + 1] == ' ')
                {
                    startOffset += 2; //remove the char and the white space

                    endOffset = Math.Min(bufferLength, startOffset + maxLength);
                    break;
                }

                endOffset++;
                startOffset--;
            }

            int retStartOffset;
            var retVal = TrimEdges(buffer, startOffset, endOffset, out retStartOffset, maxStart, minEnd); // cuts part words
            startOffset = retStartOffset;
            return retVal;
        }

        private string TrimEdges(StringBuilder buffer, int startOffset, int endOffset, out int retStartOffset, int maxStart, int minEnd)
        {
            var localStart = startOffset;
            var localEnd = endOffset;
            if (startOffset != 0 && buffer[startOffset - 1] != ' ')
            {
                while (buffer[localStart] != ' ' && localStart != endOffset && localStart < maxStart)
                {
                    localStart++;
                }

                if (localStart == endOffset)
                    localStart = startOffset;
            }

            if (endOffset + 1 < buffer.Length && buffer[endOffset + 1] != ' ')
            {
                while (buffer[localEnd] != ' ' && localEnd != localStart && localEnd > minEnd)
                {
                    localEnd--;
                }

                if (localEnd == localStart)
                {
                    localEnd = endOffset;
                    localStart = startOffset;
                }
            }

            retStartOffset = localStart;
            return buffer.ToString(localStart, localEnd - localStart);
        }

        protected virtual String GetPreTag(int num)
        {
            int n = num % preTags.Length;
            return preTags[n];
        }

        protected virtual String GetPostTag(int num)
        {
            int n = num % postTags.Length;
            return postTags[n];
        }
    }
}
