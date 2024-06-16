/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
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
using Lucene.Net.Store;
using Lucene.Net.Support;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using Directory = Lucene.Net.Store.Directory;
using Lock = Lucene.Net.Store.Lock;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;

namespace Lucene.Net.Index
{
    
    /// <summary> An IndexReader which reads indexes with multiple segments.</summary>
    public class DirectoryReader:IndexReader
    {
        /*new*/ private class AnonymousClassFindSegmentsFile:SegmentInfos.FindSegmentsFile
        {
            private void  InitBlock(bool readOnly, IndexDeletionPolicy deletionPolicy, int termInfosIndexDivisor)
            {
                this.readOnly = readOnly;
                this.deletionPolicy = deletionPolicy;
                this.termInfosIndexDivisor = termInfosIndexDivisor;
            }
            private bool readOnly;
            private IndexDeletionPolicy deletionPolicy;
            private int termInfosIndexDivisor;
            internal AnonymousClassFindSegmentsFile(bool readOnly, Lucene.Net.Index.IndexDeletionPolicy deletionPolicy, int termInfosIndexDivisor, Lucene.Net.Store.Directory Param1):base(Param1)
            {
                InitBlock(readOnly, deletionPolicy, termInfosIndexDivisor);
            }
            public /*protected internal*/ override System.Object DoBody(System.String segmentFileName, IState state)
            {
                var infos = new SegmentInfos();
                infos.Read(directory, segmentFileName, state);
                if (readOnly)
                    return new ReadOnlyDirectoryReader(directory, infos, deletionPolicy, termInfosIndexDivisor, state);
                else
                    return new DirectoryReader(directory, infos, deletionPolicy, false, termInfosIndexDivisor, state);
            }
        }
        private class AnonymousClassFindSegmentsFile1:SegmentInfos.FindSegmentsFile
        {
            private void  InitBlock(bool openReadOnly, DirectoryReader enclosingInstance)
            {
                this.openReadOnly = openReadOnly;
                this.enclosingInstance = enclosingInstance;
            }
            private bool openReadOnly;
            private DirectoryReader enclosingInstance;
            public DirectoryReader Enclosing_Instance
            {
                get
                {
                    return enclosingInstance;
                }
                
            }
            internal AnonymousClassFindSegmentsFile1(bool openReadOnly, DirectoryReader enclosingInstance, Lucene.Net.Store.Directory Param1):base(Param1)
            {
                InitBlock(openReadOnly, enclosingInstance);
            }
            public /*protected internal*/ override System.Object DoBody(System.String segmentFileName, IState state)
            {
                var infos = new SegmentInfos();
                infos.Read(directory, segmentFileName, state);
                return Enclosing_Instance.DoReopen(infos, false, openReadOnly, state);
            }
        }
        protected internal Directory internalDirectory;
        protected internal bool readOnly;
        
        internal IndexWriter writer;
        
        private IndexDeletionPolicy deletionPolicy;
        private readonly HashSet<string> synced = new HashSet<string>();
        private Lock writeLock;
        private readonly SegmentInfos segmentInfos;
        private readonly SegmentInfos segmentInfosStart;
        private bool stale;
        private readonly int termInfosIndexDivisor;
        
        private bool rollbackHasChanges;
                
        private SegmentReader[] subReaders;
        private int[] starts; // 1st docno for each segment
        private HashMap<string, byte[]> normsCache = new HashMap<string, byte[]>();
        private int maxDoc = 0;
        private int numDocs = - 1;
        private bool hasDeletions = false;
        
        // Max version in index as of when we opened; this can be
        // > our current segmentInfos version in case we were
        // opened on a past IndexCommit:
        private long maxIndexVersion;
        
        internal static IndexReader Open(Directory directory, IndexDeletionPolicy deletionPolicy, IndexCommit commit, bool readOnly, int termInfosIndexDivisor, IState state)
        {
            return (IndexReader) new AnonymousClassFindSegmentsFile(readOnly, deletionPolicy, termInfosIndexDivisor, directory).Run(commit, state);
        }
        
        /// <summary>Construct reading the named set of readers. </summary>
        internal DirectoryReader(Directory directory, SegmentInfos sis, IndexDeletionPolicy deletionPolicy, bool readOnly, int termInfosIndexDivisor, IState state)
        {
            internalDirectory = directory;
            this.readOnly = readOnly;
            this.segmentInfos = sis;
            this.deletionPolicy = deletionPolicy;
            this.termInfosIndexDivisor = termInfosIndexDivisor;
            
            if (!readOnly)
            {
                // We assume that this segments_N was previously
                // properly sync'd:
                synced.UnionWith(sis.Files(directory, true, state));
            }
            
            // To reduce the chance of hitting FileNotFound
            // (and having to retry), we open segments in
            // reverse because IndexWriter merges & deletes
            // the newest segments first.
            
            var readers = new SegmentReader[sis.Count];
            for (int i = sis.Count - 1; i >= 0; i--)
            {
                bool success = false;
                try
                {
                    readers[i] = SegmentReader.Get(readOnly, sis.Info(i), termInfosIndexDivisor, state);
                    success = true;
                }
                finally
                {
                    if (!success)
                    {
                        // Close all readers we had opened:
                        for (i++; i < sis.Count; i++)
                        {
                            try
                            {
                                readers[i].Close();
                            }
                            catch (System.Exception)
                            {
                                // keep going - we want to clean up as much as possible
                            }
                        }
                    }
                }
            }
            
            Initialize(readers, state);
        }
        
        // Used by near real-time search
        internal DirectoryReader(IndexWriter writer, SegmentInfos infos, int termInfosIndexDivisor, IState state)
        {
            this.internalDirectory = writer.Directory;
            this.readOnly = true;
            segmentInfos = infos;
            segmentInfosStart = (SegmentInfos) infos.Clone();
            this.termInfosIndexDivisor = termInfosIndexDivisor;
            if (!readOnly)
            {
                // We assume that this segments_N was previously
                // properly sync'd:
                synced.UnionWith(infos.Files(internalDirectory, true, state));
            }
            
            // IndexWriter synchronizes externally before calling
            // us, which ensures infos will not change; so there's
            // no need to process segments in reverse order
            int numSegments = infos.Count;
            var readers = new SegmentReader[numSegments];
            Directory dir = writer.Directory;
            int upto = 0;
            
            for (int i = 0; i < numSegments; i++)
            {
                bool success = false;
                try
                {
                    SegmentInfo info = infos.Info(i);
                    if (info.dir == dir)
                    {
                        readers[upto++] = writer.readerPool.GetReadOnlyClone(info, true, termInfosIndexDivisor, state);
                    }
                    success = true;
                }
                finally
                {
                    if (!success)
                    {
                        // Close all readers we had opened:
                        for (upto--; upto >= 0; upto--)
                        {
                            try
                            {
                                readers[upto].Close();
                            }
                            catch (System.Exception)
                            {
                                // keep going - we want to clean up as much as possible
                            }
                        }
                    }
                }
            }
            
            this.writer = writer;
            
            if (upto < readers.Length)
            {
                // This means some segments were in a foreign Directory
                var newReaders = new SegmentReader[upto];
                Array.Copy(readers, 0, newReaders, 0, upto);
                readers = newReaders;
            }
            
            Initialize(readers, state);
        }
        
        /// <summary>This constructor is only used for <see cref="Reopen()" /> </summary>
        internal DirectoryReader(Directory directory, SegmentInfos infos, SegmentReader[] oldReaders, int[] oldStarts,
                                 IEnumerable<KeyValuePair<string, byte[]>> oldNormsCache, bool readOnly, bool doClone, int termInfosIndexDivisor, IState state)
        {
            this.internalDirectory = directory;
            this.readOnly = readOnly;
            this.segmentInfos = infos;
            this.termInfosIndexDivisor = termInfosIndexDivisor;
            if (!readOnly)
            {
                // We assume that this segments_N was previously
                // properly sync'd:
                synced.UnionWith(infos.Files(directory, true, state));
            }

            // we put the old SegmentReaders in a map, that allows us
            // to lookup a reader using its segment name
            var segmentReaders = new HashMap<string, int>();
            
            if (oldReaders != null)
            {
                // create a Map SegmentName->SegmentReader
                for (int i = 0; i < oldReaders.Length; i++)
                {
                    segmentReaders[oldReaders[i].SegmentName] = i;
                }
            }
            
            var newReaders = new SegmentReader[infos.Count];
            
            // remember which readers are shared between the old and the re-opened
            // DirectoryReader - we have to incRef those readers
            var readerShared = new bool[infos.Count];
            
            for (int i = infos.Count - 1; i >= 0; i--)
            {
                // find SegmentReader for this segment
                if (!segmentReaders.ContainsKey(infos.Info(i).name))
                {
                    // this is a new segment, no old SegmentReader can be reused
                    newReaders[i] = null;
                }
                else
                {
                    // there is an old reader for this segment - we'll try to reopen it
                    newReaders[i] = oldReaders[segmentReaders[infos.Info(i).name]];
                }
                
                bool success = false;
                try
                {
                    SegmentReader newReader;
                    if (newReaders[i] == null || infos.Info(i).GetUseCompoundFile(state) != newReaders[i].SegmentInfo.GetUseCompoundFile(state))
                    {
                        
                        // We should never see a totally new segment during cloning
                        System.Diagnostics.Debug.Assert(!doClone);
                        
                        // this is a new reader; in case we hit an exception we can close it safely
                        newReader = SegmentReader.Get(readOnly, infos.Info(i), termInfosIndexDivisor, state);
                    }
                    else
                    {
                        newReader = newReaders[i].ReopenSegment(infos.Info(i), doClone, readOnly, state);
                    }
                    if (newReader == newReaders[i])
                    {
                        // this reader will be shared between the old and the new one,
                        // so we must incRef it
                        readerShared[i] = true;
                        newReader.IncRef();
                    }
                    else
                    {
                        readerShared[i] = false;
                        newReaders[i] = newReader;
                    }
                    success = true;
                }
                finally
                {
                    if (!success)
                    {
                        for (i++; i < infos.Count; i++)
                        {
                            if (newReaders[i] != null)
                            {
                                try
                                {
                                    if (!readerShared[i])
                                    {
                                        // this is a new subReader that is not used by the old one,
                                        // we can close it
                                        newReaders[i].Close();
                                    }
                                    else
                                    {
                                        // this subReader is also used by the old reader, so instead
                                        // closing we must decRef it
                                        newReaders[i].DecRef(state);
                                    }
                                }
                                catch (System.IO.IOException)
                                {
                                    // keep going - we want to clean up as much as possible
                                }
                            }
                        }
                    }
                }
            }
            
            // initialize the readers to calculate maxDoc before we try to reuse the old normsCache
            Initialize(newReaders, state);
            
            // try to copy unchanged norms from the old normsCache to the new one
            if (oldNormsCache != null)
            {
                foreach(var entry in oldNormsCache)
                {
                    String field = entry.Key;
                    if (!HasNorms(field, state))
                    {
                        continue;
                    }
                    
                    byte[] oldBytes = entry.Value;
                    
                    var bytes = new byte[MaxDoc];
                    
                    for (int i = 0; i < subReaders.Length; i++)
                    {
                        int oldReaderIndex = segmentReaders[subReaders[i].SegmentName];
                        
                        // this SegmentReader was not re-opened, we can copy all of its norms 
                        if (segmentReaders.ContainsKey(subReaders[i].SegmentName) &&
                             (oldReaders[oldReaderIndex] == subReaders[i]
                               || oldReaders[oldReaderIndex].norms[field] == subReaders[i].norms[field]))
                        {
                            // we don't have to synchronize here: either this constructor is called from a SegmentReader,
                            // in which case no old norms cache is present, or it is called from MultiReader.reopen(),
                            // which is synchronized
                            Array.Copy(oldBytes, oldStarts[oldReaderIndex], bytes, starts[i], starts[i + 1] - starts[i]);
                        }
                        else
                        {
                            subReaders[i].Norms(field, bytes, starts[i], state);
                        }
                    }
                    
                    normsCache[field] = bytes; // update cache
                }
            }
        }
        
        private void  Initialize(SegmentReader[] subReaders, IState state)
        {
            this.subReaders = subReaders;
            starts = new int[subReaders.Length + 1]; // build starts array
            for (int i = 0; i < subReaders.Length; i++)
            {
                starts[i] = maxDoc;
                maxDoc += subReaders[i].MaxDoc; // compute maxDocs
                
                if (subReaders[i].HasDeletions)
                    hasDeletions = true;
            }
            starts[subReaders.Length] = maxDoc;

            if (!readOnly)
            {
                maxIndexVersion = SegmentInfos.ReadCurrentVersion(internalDirectory, state);
            }
        }
        
        public override Object Clone(IState state)
        {
            lock (this)
            {
                try
                {
                    return Clone(readOnly, state); // Preserve current readOnly
                }
                catch (Exception ex)
                {
                    throw new SystemException(ex.Message, ex); // TODO: why rethrow this way?
                }
            }
        }
        
        public override IndexReader Clone(bool openReadOnly, IState state)
        {
            lock (this)
            {
                DirectoryReader newReader = DoReopen((SegmentInfos) segmentInfos.Clone(), true, openReadOnly, state);
                
                if (this != newReader)
                {
                    newReader.deletionPolicy = deletionPolicy;
                }
                newReader.writer = writer;
                // If we're cloning a non-readOnly reader, move the
                // writeLock (if there is one) to the new reader:
                if (!openReadOnly && writeLock != null)
                {
                    // In near real-time search, reader is always readonly
                    System.Diagnostics.Debug.Assert(writer == null);
                    newReader.writeLock = writeLock;
                    newReader.hasChanges = hasChanges;
                    newReader.hasDeletions = hasDeletions;
                    writeLock = null;
                    hasChanges = false;
                }
                
                return newReader;
            }
        }
        
        public override IndexReader Reopen(IState state)
        {
            // Preserve current readOnly
            return DoReopen(readOnly, null, state);
        }
        
        public override IndexReader Reopen(bool openReadOnly, IState state)
        {
            return DoReopen(openReadOnly, null, state);
        }
        
        public override IndexReader Reopen(IndexCommit commit, IState state)
        {
            return DoReopen(true, commit, state);
        }

        private IndexReader DoReopenFromWriter(bool openReadOnly, IndexCommit commit, IState state)
        {
            System.Diagnostics.Debug.Assert(readOnly);

            if (!openReadOnly)
            {
                throw new System.ArgumentException("a reader obtained from IndexWriter.getReader() can only be reopened with openReadOnly=true (got false)");
            }

            if (commit != null)
            {
                throw new System.ArgumentException("a reader obtained from IndexWriter.getReader() cannot currently accept a commit");
            }

            // TODO: right now we *always* make a new reader; in
            // the future we could have write make some effort to
            // detect that no changes have occurred
            return writer.GetReader(state);
        }

        internal virtual IndexReader DoReopen(bool openReadOnly, IndexCommit commit, IState state)
        {
            EnsureOpen();

            System.Diagnostics.Debug.Assert(commit == null || openReadOnly);

            // If we were obtained by writer.getReader(), re-ask the
            // writer to get a new reader.
            if (writer != null)
            {
                return DoReopenFromWriter(openReadOnly, commit, state);
            }
            else
            {
                return DoReopenNoWriter(openReadOnly, commit, state);
            }
        }
                
        private IndexReader DoReopenNoWriter(bool openReadOnly, IndexCommit commit, IState state)
        {
            lock (this)
            {
                if (commit == null)
                {
                    if (hasChanges)
                    {
                        // We have changes, which means we are not readOnly:
                        System.Diagnostics.Debug.Assert(readOnly == false);
                        // and we hold the write lock:
                        System.Diagnostics.Debug.Assert(writeLock != null);
                        // so no other writer holds the write lock, which
                        // means no changes could have been done to the index:
                        System.Diagnostics.Debug.Assert(IsCurrent(state));

                        if (openReadOnly)
                        {
                            return Clone(openReadOnly, state);
                        }
                        else
                        {
                            return this;
                        }
                    }
                    else if (IsCurrent(state))
                    {
                        if (openReadOnly != readOnly)
                        {
                            // Just fallback to clone
                            return Clone(openReadOnly, state);
                        }
                        else
                        {
                            return this;
                        }
                    }
                }
                else
                {
                    if (internalDirectory != commit.Directory)
                        throw new System.IO.IOException("the specified commit does not match the specified Directory");
                    if (segmentInfos != null && commit.SegmentsFileName.Equals(segmentInfos.GetCurrentSegmentFileName()))
                    {
                        if (readOnly != openReadOnly)
                        {
                            // Just fallback to clone
                            return Clone(openReadOnly, state);
                        }
                        else
                        {
                            return this;
                        }
                    }
                }

                return (IndexReader)new AnonymousFindSegmentsFile(internalDirectory, openReadOnly, this).Run(commit, state);
            }
        }

        class AnonymousFindSegmentsFile : SegmentInfos.FindSegmentsFile
        {
        	readonly DirectoryReader enclosingInstance;
        	readonly bool openReadOnly;
        	readonly Directory dir;
            public AnonymousFindSegmentsFile(Directory directory, bool openReadOnly, DirectoryReader dirReader) : base(directory)
            {
                this.dir = directory;
                this.openReadOnly = openReadOnly;
                enclosingInstance = dirReader;
            }

            public override object DoBody(string segmentFileName, IState state)
            {
                var infos = new SegmentInfos();
                infos.Read(dir, segmentFileName, state);
                return enclosingInstance.DoReopen(infos, false, openReadOnly, state);
            }
        }

        private DirectoryReader DoReopen(SegmentInfos infos, bool doClone, bool openReadOnly, IState state)
        {
            lock (this)
            {
                DirectoryReader reader;
                if (openReadOnly)
                {
                    reader = new ReadOnlyDirectoryReader(internalDirectory, infos, subReaders, starts, normsCache, doClone, termInfosIndexDivisor, state);
                }
                else
                {
                    reader = new DirectoryReader(internalDirectory, infos, subReaders, starts, normsCache, false, doClone, termInfosIndexDivisor, state);
                }
                return reader;
            }
        }


        /// <summary>Version number when this IndexReader was opened. </summary>
        public override long Version
        {
            get
            {
                EnsureOpen();
                return segmentInfos.Version;
            }
        }

        public override ITermFreqVector[] GetTermFreqVectors(int n, IState state)
        {
            EnsureOpen();
            int i = ReaderIndex(n); // find segment num
            return subReaders[i].GetTermFreqVectors(n - starts[i], state); // dispatch to segment
        }
        
        public override ITermFreqVector GetTermFreqVector(int n, System.String field, IState state)
        {
            EnsureOpen();
            int i = ReaderIndex(n); // find segment num
            return subReaders[i].GetTermFreqVector(n - starts[i], field, state);
        }
        
        
        public override void  GetTermFreqVector(int docNumber, System.String field, TermVectorMapper mapper, IState state)
        {
            EnsureOpen();
            int i = ReaderIndex(docNumber); // find segment num
            subReaders[i].GetTermFreqVector(docNumber - starts[i], field, mapper, state);
        }
        
        public override void  GetTermFreqVector(int docNumber, TermVectorMapper mapper, IState state)
        {
            EnsureOpen();
            int i = ReaderIndex(docNumber); // find segment num
            subReaders[i].GetTermFreqVector(docNumber - starts[i], mapper, state);
        }

        /// <summary> Checks is the index is optimized (if it has a single segment and no deletions)</summary>
        /// <returns> &amp;lt;c&amp;gt;true&amp;lt;/c&amp;gt; if the index is optimized; &amp;lt;c&amp;gt;false&amp;lt;/c&amp;gt; otherwise </returns>
        public override bool IsOptimized()
        {
            EnsureOpen();
            return segmentInfos.Count == 1 && !HasDeletions;
        }

        public override int NumDocs()
        {
            // Don't call ensureOpen() here (it could affect performance)
            // NOTE: multiple threads may wind up init'ing
            // numDocs... but that's harmless
            if (numDocs == - 1)
            {
                // check cache
                int n = subReaders.Sum(t => t.NumDocs()); // cache miss--recompute
            	numDocs = n;
            }
            return numDocs;
        }

        public override int MaxDoc
        {
            get
            {
                // Don't call ensureOpen() here (it could affect performance)
                return maxDoc;
            }
        }

        // inherit javadoc
        public override Document Document(int n, FieldSelector fieldSelector, IState state)
        {
            EnsureOpen();
            int i = ReaderIndex(n); // find segment num
            return subReaders[i].Document(n - starts[i], fieldSelector, state); // dispatch to segment reader
        }
        
        public override bool IsDeleted(int n)
        {
            // Don't call ensureOpen() here (it could affect performance)
            int i = ReaderIndex(n); // find segment num
            return subReaders[i].IsDeleted(n - starts[i]); // dispatch to segment reader
        }

        public override bool HasDeletions
        {
            get
            {
                // Don't call ensureOpen() here (it could affect performance)
                return hasDeletions;
            }
        }

        protected internal override void  DoDelete(int n, IState state)
        {
            numDocs = - 1; // invalidate cache
            int i = ReaderIndex(n); // find segment num
            subReaders[i].DeleteDocument(n - starts[i], state); // dispatch to segment reader
            hasDeletions = true;
        }
        
        protected internal override void  DoUndeleteAll(IState state)
        {
            foreach (SegmentReader t in subReaders)
            	t.UndeleteAll(state);

        	hasDeletions = false;
            numDocs = - 1; // invalidate cache
        }
        
        private int ReaderIndex(int n)
        {
            // find reader for doc n:
            return ReaderIndex(n, this.starts, this.subReaders.Length);
        }
        
        internal static int ReaderIndex(int n, int[] starts, int numSubReaders)
        {
            // find reader for doc n:
            int lo = 0; // search starts array
            int hi = numSubReaders - 1; // for first element less
            
            while (hi >= lo)
            {
                int mid = Number.URShift((lo + hi), 1);
                int midValue = starts[mid];
                if (n < midValue)
                    hi = mid - 1;
                else if (n > midValue)
                    lo = mid + 1;
                else
                {
                    // found a match
                    while (mid + 1 < numSubReaders && starts[mid + 1] == midValue)
                    {
                        mid++; // scan to last match
                    }
                    return mid;
                }
            }
            return hi;
        }
        
        public override bool HasNorms(System.String field, IState state)
        {
            EnsureOpen();
        	return subReaders.Any(t => t.HasNorms(field, state));
        }
        
        public override byte[] Norms(System.String field, IState state)
        {
            lock (this)
            {
                EnsureOpen();
                byte[] bytes = normsCache[field];
                if (bytes != null)
                    return bytes; // cache hit
                if (!HasNorms(field, state))
                    return null;
                
                bytes = new byte[MaxDoc];
                for (int i = 0; i < subReaders.Length; i++)
                    subReaders[i].Norms(field, bytes, starts[i], state);
                normsCache[field] = bytes; // update cache
                return bytes;
            }
        }
        
        public override void  Norms(System.String field, byte[] result, int offset, IState state)
        {
            lock (this)
            {
                EnsureOpen();
                byte[] bytes = normsCache[field];
                if (bytes == null && !HasNorms(field, state))
                {
                    byte val = DefaultSimilarity.EncodeNorm(1.0f);
                    for (int index = offset; index < result.Length; index++)
                        result.SetValue(val, index);
                }
                else if (bytes != null)
                {
                    // cache hit
                    Array.Copy(bytes, 0, result, offset, MaxDoc);
                }
                else
                {
                    for (int i = 0; i < subReaders.Length; i++)
                    {
                        // read from segments
                        subReaders[i].Norms(field, result, offset + starts[i], state);
                    }
                }
            }
        }
        
        protected internal override void  DoSetNorm(int n, System.String field, byte value_Renamed, IState state)
        {
            lock (normsCache)
            {
                normsCache.Remove(field); // clear cache      
            }
            int i = ReaderIndex(n); // find segment num
            subReaders[i].SetNorm(n - starts[i], field, value_Renamed, state); // dispatch
        }
        
        public override TermEnum Terms(IState state)
        {
            EnsureOpen();
            return new MultiTermEnum(this, subReaders, starts, null, state);
        }
        
        public override TermEnum Terms(Term term, IState state)
        {
            EnsureOpen();
            return new MultiTermEnum(this, subReaders, starts, term, state);
        }
        
        public override int DocFreq(Term t, IState state)
        {
            EnsureOpen();
            int total = 0; // sum freqs in segments
            for (int i = 0; i < subReaders.Length; i++)
                total += subReaders[i].DocFreq(t, state);
            return total;
        }
        
        public override TermDocs TermDocs(IState state)
        {
            EnsureOpen();
            return new MultiTermDocs(this, subReaders, starts);
        }
        
        public override TermPositions TermPositions(IState state)
        {
            EnsureOpen();
            return new MultiTermPositions(this, subReaders, starts);
        }
        
        /// <summary> Tries to acquire the WriteLock on this directory. this method is only valid if this IndexReader is directory
        /// owner.
        /// 
        /// </summary>
        /// <throws>  StaleReaderException  if the index has changed since this reader was opened </throws>
        /// <throws>  CorruptIndexException if the index is corrupt </throws>
        /// <throws>  Lucene.Net.Store.LockObtainFailedException </throws>
        /// <summary>                               if another writer has this index open (<c>write.lock</c> could not be
        /// obtained)
        /// </summary>
        /// <throws>  IOException           if there is a low-level IO error </throws>
        protected internal override void  AcquireWriteLock(IState state)
        {
            
            if (readOnly)
            {
                // NOTE: we should not reach this code w/ the core
                // IndexReader classes; however, an external subclass
                // of IndexReader could reach this.
                ReadOnlySegmentReader.NoWrite();
            }
            
            if (segmentInfos != null)
            {
                EnsureOpen();
                if (stale)
                    throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
                
                if (this.writeLock == null)
                {
                    Lock writeLock = internalDirectory.MakeLock(IndexWriter.WRITE_LOCK_NAME);
                    if (!writeLock.Obtain(IndexWriter.WRITE_LOCK_TIMEOUT))
                    // obtain write lock
                    {
                        throw new LockObtainFailedException("Index locked for write: " + writeLock);
                    }
                    this.writeLock = writeLock;
                    
                    // we have to check whether index has changed since this reader was opened.
                    // if so, this reader is no longer valid for
                    // deletion
                    if (SegmentInfos.ReadCurrentVersion(internalDirectory, state) > maxIndexVersion)
                    {
                        stale = true;
                        this.writeLock.Release();
                        this.writeLock = null;
                        throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
                    }
                }
            }
        }
        
        /// <summary> Commit changes resulting from delete, undeleteAll, or setNorm operations
        /// <p/>
        /// If an exception is hit, then either no changes or all changes will have been committed to the index (transactional
        /// semantics).
        /// 
        /// </summary>
        /// <throws>  IOException if there is a low-level IO error </throws>
        protected internal override void DoCommit(IDictionary<string, string> commitUserData, IState state)
        {
            if (hasChanges)
            {
                segmentInfos.UserData = commitUserData;
                // Default deleter (for backwards compatibility) is
                // KeepOnlyLastCommitDeleter:
                var deleter = new IndexFileDeleter(internalDirectory, deletionPolicy ?? new KeepOnlyLastCommitDeletionPolicy(), segmentInfos, null, null, synced, state);

                segmentInfos.UpdateGeneration(deleter.LastSegmentInfos);

                // Checkpoint the state we are about to change, in
                // case we have to roll back:
                StartCommit();
                
                bool success = false;
                try
                {
                    foreach (SegmentReader t in subReaders)
                    	t.Commit(state);

                	// Sync all files we just wrote
                    foreach(string fileName in segmentInfos.Files(internalDirectory, false, state))
                    {
                        if(!synced.Contains(fileName))
                        {
                            System.Diagnostics.Debug.Assert(internalDirectory.FileExists(fileName, state));
                            internalDirectory.Sync(fileName);
                            synced.Add(fileName);
                        }   
                    }
                    
                    segmentInfos.Commit(internalDirectory, state);
                    success = true;
                }
                finally
                {
                    
                    if (!success)
                    {
                        
                        // Rollback changes that were made to
                        // SegmentInfos but failed to get [fully]
                        // committed.  This way this reader instance
                        // remains consistent (matched to what's
                        // actually in the index):
                        RollbackCommit();
                        
                        // Recompute deletable files & remove them (so
                        // partially written .del files, etc, are
                        // removed):
                        deleter.Refresh(state);
                    }
                }
                
                // Have the deleter remove any now unreferenced
                // files due to this commit:
                deleter.Checkpoint(segmentInfos, true, state);
                deleter.Dispose();

                maxIndexVersion = segmentInfos.Version;
                
                if (writeLock != null)
                {
                    writeLock.Release(); // release write lock
                    writeLock = null;
                }
            }
            hasChanges = false;
        }
        
        internal virtual void  StartCommit()
        {
        	rollbackHasChanges = hasChanges;
        	foreach (SegmentReader t in subReaders)
        	{
        		t.StartCommit();
        	}
        }

    	internal virtual void  RollbackCommit()
    	{
    		hasChanges = rollbackHasChanges;
    		foreach (SegmentReader t in subReaders)
    		{
    			t.RollbackCommit();
    		}
    	}

    	public override IDictionary<string, string> CommitUserData
        {
            get
            {
                EnsureOpen();
                return segmentInfos.UserData;
            }
        }

        public override bool IsCurrent(IState state)
        {
            EnsureOpen();
            if (writer == null || writer.IsClosed())
            {
                // we loaded SegmentInfos from the directory
                return SegmentInfos.ReadCurrentVersion(internalDirectory, state) == segmentInfos.Version;
            }
            else
            {
                return writer.NrtIsCurrent(segmentInfosStart);
            }
        }

        protected internal override void  DoClose(IState state)
        {
            lock (this)
            {
                System.IO.IOException ioe = null;
                normsCache = null;
                foreach (SegmentReader t in subReaders)
                {
					// try to close each reader, even if an exception is thrown
                	try
                	{
                		t.DecRef(state);
                	}
                	catch (System.IO.IOException e)
                	{
                		if (ioe == null)
                			ioe = e;
                	}
                }

            	// NOTE: only needed in case someone had asked for
                // FieldCache for top-level reader (which is generally
                // not a good idea):
                Search.FieldCache_Fields.DEFAULT.Purge(this);

                // throw the first exception
                if (ioe != null)
                    throw ioe;
            }
        }

        public override ICollection<string> GetFieldNames(IndexReader.FieldOption fieldNames)
        {
            EnsureOpen();
            return GetFieldNames(fieldNames, this.subReaders);
        }

        internal static ICollection<string> GetFieldNames(IndexReader.FieldOption fieldNames, IndexReader[] subReaders)
        {
            // maintain a unique set of field names
            ISet<string> fieldSet = Support.Compatibility.SetFactory.CreateHashSet<string>();
            foreach (IndexReader reader in subReaders)
            {
                fieldSet.UnionWith(reader.GetFieldNames(fieldNames));
            }
            return fieldSet;
        }

        public override IndexReader[] GetSequentialSubReaders()
        {
            return subReaders;
        }

        /// <summary>Returns the directory this index resides in. </summary>
        public override Directory Directory()
        {
            // Don't ensureOpen here -- in certain cases, when a
            // cloned/reopened reader needs to commit, it may call
            // this method on the closed original reader
            return internalDirectory;
        }

        public override int TermInfosIndexDivisor
        {
            get { return termInfosIndexDivisor; }
        }

        /// <summary> Expert: return the IndexCommit that this reader has opened.
        /// <p/>
        /// <p/><b>WARNING</b>: this API is new and experimental and may suddenly change.<p/>
        /// </summary>
        public override IndexCommit IndexCommit(IState state)
        {
            return new ReaderCommit(segmentInfos, internalDirectory, state);
        }

        /// <seealso cref="Lucene.Net.Index.IndexReader.ListCommits">
        /// </seealso>
        public static new ICollection<IndexCommit> ListCommits(Directory dir, IState state)
        {
            String[] files = dir.ListAll(state);

            ICollection<IndexCommit> commits = new  List<IndexCommit>();
            
            var latest = new SegmentInfos();
            latest.Read(dir, state);
            long currentGen = latest.Generation;
            
            commits.Add(new ReaderCommit(latest, dir, state));
            
            foreach (string fileName in files)
            {
            	if (fileName.StartsWith(IndexFileNames.SEGMENTS) && !fileName.Equals(IndexFileNames.SEGMENTS_GEN) && SegmentInfos.GenerationFromSegmentsFileName(fileName) < currentGen)
            	{
                    
            		var sis = new SegmentInfos();
            		try
            		{
            			// IOException allowed to throw there, in case
            			// segments_N is corrupt
            			sis.Read(dir, fileName, state);
            		}
            		catch (System.IO.FileNotFoundException)
            		{
            			// LUCENE-948: on NFS (and maybe others), if
            			// you have writers switching back and forth
            			// between machines, it's very likely that the
            			// dir listing will be stale and will claim a
            			// file segments_X exists when in fact it
            			// doesn't.  So, we catch this and handle it
            			// as if the file does not exist
            			sis = null;
            		}
                    
            		if (sis != null)
            			commits.Add(new ReaderCommit(sis, dir, state));
            	}
            }
            
            return commits;
        }
        
        private sealed class ReaderCommit:IndexCommit
        {
            private readonly String segmentsFileName;
        	private readonly ICollection<string> files;
        	private readonly Directory dir;
        	private readonly long generation;
        	private readonly long version;
        	private readonly bool isOptimized;
        	private readonly IDictionary<string, string> userData;
            
            internal ReaderCommit(SegmentInfos infos, Directory dir, IState state)
            {
                segmentsFileName = infos.GetCurrentSegmentFileName();
                this.dir = dir;
                userData = infos.UserData;
                files = infos.Files(dir, true, state);
                version = infos.Version;
                generation = infos.Generation;
                isOptimized = infos.Count == 1 && !infos.Info(0).HasDeletions(state);
            }
            public override string ToString()
            {
                return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
            }

            public override bool IsOptimized
            {
                get { return isOptimized; }
            }

            public override string SegmentsFileName
            {
                get { return segmentsFileName; }
            }

            public override ICollection<string> FileNames
            {
                get { return files; }
            }

            public override Directory Directory
            {
                get { return dir; }
            }

            public override long Version
            {
                get { return version; }
            }

            public override long Generation
            {
                get { return generation; }
            }

            public override bool IsDeleted
            {
                get { return false; }
            }

            public override IDictionary<string, string> UserData
            {
                get { return userData; }
            }

            public override void Delete()
            {
                throw new System.NotSupportedException("This IndexCommit does not support deletions");
            }
        }
        
        internal class MultiTermEnum:TermEnum
        {
            internal IndexReader topReader; // used for matching TermEnum to TermDocs
            private readonly SegmentMergeQueue queue;
            
            private Term term;
            private int docFreq;
            internal SegmentMergeInfo[] matchingSegments; // null terminated array of matching segments
            
            public MultiTermEnum(IndexReader topReader, IndexReader[] readers, int[] starts, Term t, IState state)
            {
                this.topReader = topReader;
                queue = new SegmentMergeQueue(readers.Length);
                matchingSegments = new SegmentMergeInfo[readers.Length + 1];
                for (int i = 0; i < readers.Length; i++)
                {
                    IndexReader reader = readers[i];

                	TermEnum termEnum = t != null ? reader.Terms(t, state) : reader.Terms(state);

                	var smi = new SegmentMergeInfo(starts[i], termEnum, reader) {ord = i};
                	if (t == null?smi.Next(state):termEnum.Term != null)
                        queue.Add(smi);
                    // initialize queue
                    else
                        smi.Dispose();
                }
                
                if (t != null && queue.Size() > 0)
                {
                    Next(state);
                }
            }
            
            public override bool Next(IState state)
            {
                foreach (SegmentMergeInfo smi in matchingSegments)
                {
                	if (smi == null)
                		break;
                	if (smi.Next(state))
                		queue.Add(smi);
                	else
                		smi.Dispose(); // done with segment
                }
                
                int numMatchingSegments = 0;
                matchingSegments[0] = null;
                
                SegmentMergeInfo top = queue.Top();
                
                if (top == null)
                {
                    term = null;
                    return false;
                }
                
                term = top.term;
                docFreq = 0;
                
                while (top != null && term.CompareTo(top.term) == 0)
                {
                    matchingSegments[numMatchingSegments++] = top;
                    queue.Pop();
                    docFreq += top.termEnum.DocFreq(); // increment freq
                    top = queue.Top();
                }
                
                matchingSegments[numMatchingSegments] = null;
                return true;
            }

            public override Term Term
            {
                get { return term; }
            }

            public override int DocFreq()
            {
                return docFreq;
            }
            
            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    queue.Dispose();
                }
            }
        }
        
        internal class MultiTermDocs : TermDocs
        {
            internal IndexReader topReader; // used for matching TermEnum to TermDocs
            protected internal IndexReader[] readers;
            protected internal int[] starts;
            protected internal Term term;
            
            protected internal int base_Renamed = 0;
            protected internal int pointer = 0;
            
            private readonly TermDocs[] readerTermDocs;
            protected internal TermDocs current; // == readerTermDocs[pointer]
            
            private MultiTermEnum tenum; // the term enum used for seeking... can be null
            internal int matchingSegmentPos; // position into the matching segments from tenum
            internal SegmentMergeInfo smi; // current segment mere info... can be null
            
            public MultiTermDocs(IndexReader topReader, IndexReader[] r, int[] s)
            {
                this.topReader = topReader;
                readers = r;
                starts = s;
                
                readerTermDocs = new TermDocs[r.Length];
            }

            public virtual int Doc
            {
                get { return base_Renamed + current.Doc; }
            }

            public virtual int Freq
            {
                get { return current.Freq; }
            }

            public virtual void  Seek(Term term, IState state)
            {
                this.term = term;
                this.base_Renamed = 0;
                this.pointer = 0;
                this.current = null;
                this.tenum = null;
                this.smi = null;
                this.matchingSegmentPos = 0;
            }
            
            public virtual void  Seek(TermEnum termEnum, IState state)
            {
                Seek(termEnum.Term, state);
            	var multiTermEnum = termEnum as MultiTermEnum;
            	if (multiTermEnum != null)
            	{
            		tenum = multiTermEnum;
            		if (topReader != tenum.topReader)
            			tenum = null;
            	}
            }
            
            public virtual bool Next(IState state)
            {
                for (; ; )
                {
                    if (current != null && current.Next(state))
                    {
                        return true;
                    }
                    else if (pointer < readers.Length)
                    {
                        if (tenum != null)
                        {
                            smi = tenum.matchingSegments[matchingSegmentPos++];
                            if (smi == null)
                            {
                                pointer = readers.Length;
                                return false;
                            }
                            pointer = smi.ord;
                        }
                        base_Renamed = starts[pointer];
                        current = TermDocs(pointer++, state);
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            
            /// <summary>Optimized implementation. </summary>
            public virtual int Read(Span<int> docs, Span<int> freqs, IState state)
            {
                while (true)
                {
                    while (current == null)
                    {
                        if (pointer < readers.Length)
                        {
                            // try next segment
                            if (tenum != null)
                            {
                                smi = tenum.matchingSegments[matchingSegmentPos++];
                                if (smi == null)
                                {
                                    pointer = readers.Length;
                                    return 0;
                                }
                                pointer = smi.ord;
                            }
                            base_Renamed = starts[pointer];
                            current = TermDocs(pointer++, state);
                        }
                        else
                        {
                            return 0;
                        }
                    }
                    int end = current.Read(docs, freqs, state);
                    if (end == 0)
                    {
                        // none left in segment
                        current = null;
                    }
                    else
                    {
                        // got some
                        int b = base_Renamed; // adjust doc numbers
                        for (int i = 0; i < end; i++)
                            docs[i] += b;
                        return end;
                    }
                }
            }
            
            /* A Possible future optimization could skip entire segments */
            public virtual bool SkipTo(int target, IState state)
            {
                for (; ; )
                {
                    if (current != null && current.SkipTo(target - base_Renamed, state))
                    {
                        return true;
                    }
                    else if (pointer < readers.Length)
                    {
                        if (tenum != null)
                        {
                            SegmentMergeInfo smi = tenum.matchingSegments[matchingSegmentPos++];
                            if (smi == null)
                            {
                                pointer = readers.Length;
                                return false;
                            }
                            pointer = smi.ord;
                        }
                        base_Renamed = starts[pointer];
                        current = TermDocs(pointer++, state);
                    }
                    else
                        return false;
                }
            }
            
            private TermDocs TermDocs(int i, IState state)
            {
                TermDocs result = readerTermDocs[i] ?? (readerTermDocs[i] = TermDocs(readers[i], state));
            	if (smi != null)
                {
                    System.Diagnostics.Debug.Assert((smi.ord == i));
                    System.Diagnostics.Debug.Assert((smi.termEnum.Term.Equals(term)));
                    result.Seek(smi.termEnum, state);
                }
                else
                {
                    result.Seek(term, state);
                }
                return result;
            }
            
            protected internal virtual TermDocs TermDocs(IndexReader reader, IState state)
            {
                return term == null ? reader.TermDocs(null, state):reader.TermDocs(state);
            }
            
            public virtual void  Close()
            {
                Dispose();
            }

            public virtual void Dispose()
            {
                Dispose(true);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (disposing)
                {
                    foreach (TermDocs t in readerTermDocs)
                    {
                    	if (t != null)
                    		t.Close();
                    }
                }
            }
        }
        
        internal class MultiTermPositions:MultiTermDocs, TermPositions
        {
            public MultiTermPositions(IndexReader topReader, IndexReader[] r, int[] s):base(topReader, r, s)
            {
            }
            
            protected internal override TermDocs TermDocs(IndexReader reader, IState state)
            {
                return reader.TermPositions(state);
            }
            
            public virtual int NextPosition(IState state)
            {
                return ((TermPositions) current).NextPosition(state);
            }

            public virtual int PayloadLength
            {
                get { return ((TermPositions) current).PayloadLength; }
            }

            public virtual byte[] GetPayload(byte[] data, int offset, IState state)
            {
                return ((TermPositions) current).GetPayload(data, offset, state);
            }
            
            
            // TODO: Remove warning after API has been finalized

            public virtual bool IsPayloadAvailable
            {
                get { return ((TermPositions) current).IsPayloadAvailable; }
            }
        }
    }
}