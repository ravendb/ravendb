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
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Lucene.Net.Analysis.Hunspell {
    public class HunspellDictionary {
        private static readonly HunspellWord NoFlags = new HunspellWord();

        private static readonly String PREFIX_KEY = "PFX";
        private static readonly String SUFFIX_KEY = "SFX";
        private static readonly String FLAG_KEY = "FLAG";
        private static readonly String AF_KEY = "AF";

        private static readonly String NUM_FLAG_TYPE = "num";
        private static readonly String UTF8_FLAG_TYPE = "UTF-8";
        private static readonly String LONG_FLAG_TYPE = "long";

        private static readonly String PREFIX_CONDITION_REGEX_PATTERN = @"^{0}";
        private static readonly String SUFFIX_CONDITION_REGEX_PATTERN = @"{0}$";

        private readonly Dictionary<String, List<HunspellAffix>> _prefixes = new Dictionary<String, List<HunspellAffix>>();
        private readonly Dictionary<String, List<HunspellAffix>> _suffixes = new Dictionary<String, List<HunspellAffix>>();
        private readonly Dictionary<String, List<HunspellWord>> _words = new Dictionary<String, List<HunspellWord>>();
        private readonly Dictionary<String, Char[]> _aliases = new Dictionary<String, Char[]>();
        private FlagParsingStrategy _flagParsingStrategy = new SimpleFlagParsingStrategy(); // Default flag parsing strategy

        /// <summary>
        ///   Creates a new HunspellDictionary containing the information read from the provided streams to hunspell affix and dictionary file.
        /// </summary>
        /// <param name = "affix">Stream for reading the hunspell affix file.</param>
        /// <param name = "dictionary">Stream for reading the hunspell dictionary file.</param>
        /// <exception cref = "IOException">Can be thrown while reading from the streams.</exception>
        /// <exception cref = "InvalidDataException">Can be thrown if the content of the files does not meet expected formats.</exception>
        public HunspellDictionary(Stream affix, Stream dictionary)
            : this(affix, new[] { dictionary }) {
        }

        /// <summary>
        ///   Creates a new HunspellDictionary containing the information read from the provided streams to hunspell affix and dictionary files.
        /// </summary>
        /// <param name = "affix">Stream for reading the hunspell affix file.</param>
        /// <param name = "dictionaries">Streams for reading the hunspell dictionary file.</param>
        /// <exception cref = "IOException">Can be thrown while reading from the streams.</exception>
        /// <exception cref = "InvalidDataException">Can be thrown if the content of the files does not meet expected formats.</exception>
        public HunspellDictionary(Stream affix, IEnumerable<Stream> dictionaries) {
            if (affix == null) throw new ArgumentNullException("affix");
            if (dictionaries == null) throw new ArgumentNullException("dictionaries");

            var encodingName = ReadDictionaryEncoding(affix);
            var encoding = Encoding.GetEncoding(encodingName);

            ReadAffixFile(affix, encoding);
            foreach (var dictionary in dictionaries)
                ReadDictionaryFile(dictionary, encoding);
        }

        /// <summary>
        ///   Looks up HunspellWords that match the String created from the given char array, offset and length.
        /// </summary>
        public IEnumerable<HunspellWord> LookupWord(String word) {
            if (word == null) throw new ArgumentNullException("word");

            List<HunspellWord> list;
            if (_words.TryGetValue(word, out list))
                return list;

            return null;
        }

        /// <summary>
        ///   Looks up HunspellAffix prefixes that have an append that matches the String created from the given char array, offset and length.
        /// </summary>
        /// <param name="word">Char array to generate the String from.</param>
        /// <param name="offset">Offset in the char array that the String starts at.</param>
        /// <param name="length">Length from the offset that the String is.</param>
        /// <returns>List of HunspellAffix prefixes with an append that matches the String, or <c>null</c> if none are found.</returns>
        public IEnumerable<HunspellAffix> LookupPrefix(char[] word, int offset, int length) {
            if (word == null) throw new ArgumentNullException("word");
            var key = new String(word, offset, length);

            List<HunspellAffix> list;
            if (_prefixes.TryGetValue(key, out list))
                return list;

            return null;
        }

        /// <summary>
        ///   Looks up HunspellAffix suffixes that have an append that matches the String created from the given char array, offset and length.
        /// </summary>
        /// <param name="word">Char array to generate the String from.</param>
        /// <param name="offset">Offset in the char array that the String starts at.</param>
        /// <param name="length">Length from the offset that the String is.</param>
        /// <returns>List of HunspellAffix suffixes with an append that matches the String, or <c>null</c> if none are found</returns>
        public IEnumerable<HunspellAffix> LookupSuffix(char[] word, int offset, int length) {
            if (word == null) throw new ArgumentNullException("word");
            var key = new String(word, offset, length);

            List<HunspellAffix> list;
            if (_suffixes.TryGetValue(key, out list))
                return list;

            return null;
        }

        /// <summary>
        ///   Reads the affix file through the provided Stream, building up the prefix and suffix maps.
        /// </summary>
        /// <param name="affixStream">Stream to read the content of the affix file from.</param>
        /// <param name="encoding">Encoding to decode the content of the file.</param>
        /// <exception cref="IOException">IOException Can be thrown while reading from the Stream.</exception>
        private void ReadAffixFile(Stream affixStream, Encoding encoding) {
            if (affixStream == null) throw new ArgumentNullException("affixStream");
            if (encoding == null) throw new ArgumentNullException("encoding");

            using (var reader = new StreamReader(affixStream, encoding)) {
                String line;
                while ((line = reader.ReadLine()) != null) {
                    if (line.StartsWith(PREFIX_KEY)) {
                        ParseAffix(_prefixes, line, reader, PREFIX_CONDITION_REGEX_PATTERN);
                    } else if (line.StartsWith(SUFFIX_KEY)) {
                        ParseAffix(_suffixes, line, reader, SUFFIX_CONDITION_REGEX_PATTERN);
                    } else if (line.StartsWith(FLAG_KEY)) {
                        // Assume that the FLAG line comes before any prefix or suffixes
                        // Store the strategy so it can be used when parsing the dic file
                        _flagParsingStrategy = GetFlagParsingStrategy(line);
                    } else if (line.StartsWith(AF_KEY)) {
                        // Parse Alias Flag
                        ParseAliasFlag(line, reader);
                    }
                }
            }
        }

        /// <summary>
        /// Parse alias flag and put it in hash
        /// </summary>
        /// <param name="line"></param>
        /// <param name="reader"></param>
        private void ParseAliasFlag(String line, TextReader reader) {
            if (reader == null) throw new ArgumentNullException("reader");
            var args = Regex.Split(line, "\\s+");
            var numLines = Int32.Parse(args[1]);

            for (var i = 0; i < numLines; i++) {
                line = reader.ReadLine();
                var ruleArgs = Regex.Split(line, "\\s+");

                if (ruleArgs[0] != "AF")
                    throw new Exception("File corrupted, should be AF directive : " + line);

                var appendFlags = _flagParsingStrategy.ParseFlags(ruleArgs[1]);
                _aliases.Add((i+1).ToString(CultureInfo.InvariantCulture), appendFlags);
            }
        }

        /// <summary>
        ///   Parses a specific affix rule putting the result into the provided affix map.
        /// </summary>
        /// <param name="affixes">Map where the result of the parsing will be put.</param>
        /// <param name="header">Header line of the affix rule.</param>
        /// <param name="reader">TextReader to read the content of the rule from.</param>
        /// <param name="conditionPattern">Pattern to be used to generate the condition regex pattern.</param>
        private void ParseAffix(Dictionary<String, List<HunspellAffix>> affixes, String header, TextReader reader, String conditionPattern) {
            if (affixes == null) throw new ArgumentNullException("affixes");
            if (header == null) throw new ArgumentNullException("header");
            if (reader == null) throw new ArgumentNullException("reader");
            if (conditionPattern == null) throw new ArgumentNullException("conditionPattern");

            var args = Regex.Split(header, "\\s+");
            var crossProduct = args[2].Equals("Y");
            var numLines = Int32.Parse(args[3]);

            var hasAliases = _aliases.Count > 0;
            for (var i = 0; i < numLines; i++) {
                var line = reader.ReadLine();
                var ruleArgs = Regex.Split(line, "\\s+");

                var affix = new HunspellAffix();

                affix.Flag = _flagParsingStrategy.ParseFlag(ruleArgs[1]);
                affix.Strip = (ruleArgs[2] == "0") ? "" : ruleArgs[2];

                var affixArg = ruleArgs[3];

                var flagSep = affixArg.LastIndexOf('/');
                if (flagSep != -1) {
                    var cflag = affixArg.Substring(flagSep + 1);
                    var appendFlags = hasAliases ? _aliases[cflag] : _flagParsingStrategy.ParseFlags(cflag);
                    Array.Sort(appendFlags);
                    affix.AppendFlags = appendFlags;
                    affix.Append = affixArg.Substring(0, flagSep);
                } else {
                    affix.Append = affixArg;
                }

                var condition = ruleArgs[4];
                affix.SetCondition(condition, String.Format(conditionPattern, condition));
                affix.IsCrossProduct = crossProduct;

                List<HunspellAffix> list;
                if (!affixes.TryGetValue(affix.Append, out list))
                    affixes.Add(affix.Append, list = new List<HunspellAffix>());

                list.Add(affix);
            }
        }

        /// <summary>
        ///   Parses the encoding specificed in the affix file readable through the provided Stream.
        /// </summary>
        /// <param name="affix">Stream for reading the affix file.</param>
        /// <returns>Encoding specified in the affix file.</returns>
        /// <exception cref="InvalidDataException">
        ///   Thrown if the first non-empty non-comment line read from the file does not
        ///   adhere to the format <c>SET encoding</c>.
        /// </exception>
        private static String ReadDictionaryEncoding(Stream affix) {
            if (affix == null) throw new ArgumentNullException("affix");

            var builder = new StringBuilder();
            for (; ; ) {
                builder.Length = 0;
                int ch;
                while ((ch = affix.ReadByte()) >= 0) {
                    if (ch == '\n') {
                        break;
                    }
                    if (ch != '\r') {
                        builder.Append((char)ch);
                    }
                }

                if (builder.Length == 0 ||
                    builder[0] == '#' ||
                    // this test only at the end as ineffective but would allow lines only containing spaces:
                    builder.ToString().Trim().Length == 0
                    ) {
                    if (ch < 0)
                        throw new InvalidDataException("Unexpected end of affix file.");

                    continue;
                }

                if ("SET ".Equals(builder.ToString(0, 4))) {
                    // cleanup the encoding string, too (whitespace)
                    return builder.ToString(4, builder.Length - 4).Trim();
                }

                throw new InvalidDataException("The first non-comment line in the affix file must " +
                                               "be a 'SET charset', was: '" + builder + "'");
            }
        }

        /// <summary>
        ///   Determines the appropriate {@link FlagParsingStrategy} based on the FLAG definiton line taken from the affix file.
        /// </summary>
        /// <param name="flagLine">Line containing the flag information</param>
        /// <returns>FlagParsingStrategy that handles parsing flags in the way specified in the FLAG definition.</returns>
        private static FlagParsingStrategy GetFlagParsingStrategy(String flagLine) {
            if (flagLine == null) throw new ArgumentNullException("flagLine");
            var flagType = flagLine.Substring(5);

            if (NUM_FLAG_TYPE.Equals(flagType))
                return new NumFlagParsingStrategy();

            if (UTF8_FLAG_TYPE.Equals(flagType))
                return new SimpleFlagParsingStrategy();

            if (LONG_FLAG_TYPE.Equals(flagType))
                return new DoubleASCIIFlagParsingStrategy();

            throw new ArgumentException("Unknown flag type: " + flagType);
        }

        /// <summary>
        ///   Reads the dictionary file through the provided Stream, building up the words map.
        /// </summary>
        /// <param name="dictionary">Stream to read the dictionary file through.</param>
        /// <param name="encoding">Encoding used to decode the contents of the file.</param>
        /// <exception cref="IOException">Can be thrown while reading from the file.</exception>
        private void ReadDictionaryFile(Stream dictionary, Encoding encoding) {
            if (dictionary == null) throw new ArgumentNullException("dictionary");
            if (encoding == null) throw new ArgumentNullException("encoding");
            var reader = new StreamReader(dictionary, encoding);

            // nocommit, don't create millions of strings.
            var line = reader.ReadLine(); // first line is number of entries
            var numEntries = Int32.Parse(line);
            var hasAliases = _aliases.Count > 0;

            // nocommit, the flags themselves can be double-chars (long) or also numeric
            // either way the trick is to encode them as char... but they must be parsed differently
            while ((line = reader.ReadLine()) != null) {
                String entry;
                HunspellWord wordForm;

                var flagSep = line.LastIndexOf('/');
                if (flagSep == -1) {
                    wordForm = NoFlags;
                    entry = line;
                } else {
                    // note, there can be comments (morph description) after a flag.
                    // we should really look for any whitespace
                    var end = line.IndexOf('\t', flagSep);
                    var cflag = end == -1 ? line.Substring(flagSep + 1) : line.Substring(flagSep + 1, end - flagSep - 1);

                    wordForm = new HunspellWord(hasAliases ? _aliases[cflag] : _flagParsingStrategy.ParseFlags(cflag));

                    entry = line.Substring(0, flagSep);
                }

                List<HunspellWord> entries;
                if (!_words.TryGetValue(entry, out entries))
                    _words.Add(entry, entries = new List<HunspellWord>());

                entries.Add(wordForm);
            }
        }

        #region Nested type: DoubleASCIIFlagParsingStrategy

        /// <summary>
        ///   Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded as
        ///   two ASCII characters whose codes must be combined into a single character.
        /// </summary>
        private class DoubleASCIIFlagParsingStrategy : FlagParsingStrategy {
            public override Char[] ParseFlags(String rawFlags) {
                if (rawFlags.Length == 0)
                    return new Char[0];

                var builder = new StringBuilder();
                for (var i = 0; i < rawFlags.Length; i += 2) {
                    var cookedFlag = (Char)(rawFlags[i] + rawFlags[i + 1]);
                    builder.Append(cookedFlag);
                }

                return builder.ToString().ToCharArray();
            }
        }

        #endregion

        #region Nested type: FlagParsingStrategy
        /// <summary>
        ///   Abstraction of the process of parsing flags taken from the affix and dic files
        /// </summary>
        private abstract class FlagParsingStrategy {
            /// <summary>
            ///   Parses the given String into a single flag.
            /// </summary>
            /// <param name="rawFlag">String to parse into a flag.</param>
            /// <returns>Parsed flag.</returns>
            public Char ParseFlag(String rawFlag) {
                if (rawFlag == null)
                    throw new ArgumentNullException("rawFlag");

                return ParseFlags(rawFlag)[0];
            }

            /// <summary>
            ///   Parses the given String into multiple flag.
            /// </summary>
            /// <param name="rawFlags">String to parse into a flags.</param>
            /// <returns>Parsed flags.</returns>
            public abstract Char[] ParseFlags(String rawFlags);
        }

        #endregion

        #region Nested type: NumFlagParsingStrategy

        /// <summary>
        ///   Implementation of {@link FlagParsingStrategy} that assumes each flag is encoded in its
        ///   numerical form.  In the case of multiple flags, each number is separated by a comma.
        /// </summary>
        private class NumFlagParsingStrategy : FlagParsingStrategy {
            public override Char[] ParseFlags(String rawFlags) {
                var rawFlagParts = rawFlags.Trim().Split(',');
                var flags = new Char[rawFlagParts.Length];

                for (var i = 0; i < rawFlagParts.Length; i++) {
                    // note, removing the trailing X/leading I for nepali... what is the rule here?! 
                    var replaced = Regex.Replace(rawFlagParts[i], "[^0-9]", "");
                    flags[i] = (Char)Int32.Parse(replaced);
                }

                return flags;
            }
        }

        #endregion

        #region Nested type: SimpleFlagParsingStrategy

        /// <summary>
        ///   Simple implementation of {@link FlagParsingStrategy} that treats the chars in each
        ///   String as a individual flags. Can be used with both the ASCII and UTF-8 flag types.
        /// </summary>
        private class SimpleFlagParsingStrategy : FlagParsingStrategy {
            public override Char[] ParseFlags(String rawFlags) {
                return rawFlags.ToCharArray();
            }
        }

        #endregion
    }
}