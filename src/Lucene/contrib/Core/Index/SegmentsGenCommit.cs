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
using System.Linq;
using System.Text;

using Lucene.Net.Store;

namespace Lucene.Net.Index
{
    /// <summary>
    /// Class that will force an index writer to open an index based
    /// on the generation in the segments.gen file as opposed to the
    /// highest generation found in a directory listing.
    /// 
    /// A use case for using this IndexCommit when opening an IndexWriter
    /// would be if index snapshots (source) are being copied over an 
    /// existing index (target) and the source now has a lower generation
    /// than the target due to initiating a rebuild of the index. 
    /// </summary>
    public class SegmentsGenCommit : IndexCommit
    {
        /// <summary>
        /// The index.
        /// </summary>
        private Directory directory;

        /// <summary>
        /// The generation to use.
        /// </summary>
        private long generation = long.MinValue;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="d">The index directory.</param>
        public SegmentsGenCommit(Directory d)
        {
            this.directory = d;
            this.ReadDirectory();
        }

        /// <summary>
        /// Get the segments_n file for the generation found in the 
        /// segments.gen file.
        /// </summary>
        /// <returns>The segments_n file to use.</returns>
        public override string SegmentsFileName
        {
            get
            {
                return IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, string.Empty, this.generation);
            }
        }

        public override long Generation
        {
            get
            {
                return this.generation;
            }
        }

        public override ICollection<string> FileNames
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override Directory Directory
        {
            get
            {
                return this.directory;
            }
        }

        /// <summary>
        /// Read the segments.gen file to get the generation number.
        /// </summary>
        private void ReadDirectory()
        {
            IndexInput genInput = null;
            try
            {
                genInput = directory.OpenInput(IndexFileNames.SEGMENTS_GEN);

                if (genInput != null)
                {
                    int version = genInput.ReadInt();
                    if (version == Lucene.Net.Index.SegmentInfos.FORMAT_LOCKLESS)
                    {
                        long gen0 = genInput.ReadLong();
                        long gen1 = genInput.ReadLong();
                        if (gen0 == gen1)
                        {
                            // The file is consistent, use the generation
                            this.generation = gen0;
                        }
                    }
                }
            }
            finally
            {
                genInput.Close();
            }
        }

        // TODO: implement these new API functions -thoward
        public override void Delete()
        {
            throw new NotImplementedException();
        }

        public override bool IsDeleted
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsOptimized
        {
            get { throw new NotImplementedException(); }
        }

        public override long Version
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override IDictionary<string, string> UserData
        {
            get
            {
                throw new NotImplementedException();
            }
        }
    }
}
