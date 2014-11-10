using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.FileSystem.Util;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;

namespace Raven.Database.FileSystem.Search
{
    /// <summary>
    /// 	Thread safe, single instance for the entire application
    /// </summary>
    public class IndexStorage : CriticalFinalizerObject, IDisposable
	{
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private static readonly ILog startupLog = LogManager.GetLogger(typeof(IndexStorage).FullName + ".Startup");

		private const string DateIndexFormat = "yyyy-MM-dd_HH-mm-ss";
		private static readonly string[] NumericIndexFields = new[] { "__size_numeric" };

        private readonly FileStream crashMarker;
        private readonly InMemoryRavenConfiguration configuration;

		private readonly string path;
		private FSDirectory directory;
		private LowerCaseKeywordAnalyzer analyzer;
		private IndexWriter writer;
        private SnapshotDeletionPolicy snapshotter;
		private readonly object writerLock = new object();
		private readonly IndexSearcherHolder currentIndexSearcherHolder = new IndexSearcherHolder();

        private bool resetIndexOnUncleanShutdown = false;

        public IndexStorage(InMemoryRavenConfiguration configuration)
		{
            if (configuration == null)
                throw new ArgumentNullException("configuration");

            this.configuration = configuration;

            try
            {
                this.path = configuration.FileSystem.IndexStoragePath;

                if (System.IO.Directory.Exists(path) == false)
                    System.IO.Directory.CreateDirectory(path);

                var crashMarkerPath = Path.Combine(path, "indexing.crash-marker");

                if (File.Exists(crashMarkerPath))
                {
                    // the only way this can happen is if we crashed because of a power outage
                    // in this case, we consider all open indexes to be corrupt and force them
                    // to be reset. This is because to get better perf, we don't flush the files to disk,
                    // so in the case of a power outage, we can't be sure that there wasn't still stuff in
                    // the OS buffer that wasn't written yet.

                    resetIndexOnUncleanShutdown = true;
                }

                // The delete on close ensures that the only way this file will exists is if there was
                // a power outage while the server was running.
                crashMarker = File.Create(crashMarkerPath, 16, FileOptions.DeleteOnClose);

                this.directory = FSDirectory.Open(new DirectoryInfo(path));
                if (IndexWriter.IsLocked(directory))
                    IndexWriter.Unlock(directory);

                this.analyzer = new LowerCaseKeywordAnalyzer();
                this.snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
                this.writer = new IndexWriter(directory, analyzer, snapshotter, IndexWriter.MaxFieldLength.UNLIMITED);
                this.writer.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());
                
                this.currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true));
            }
            catch
            {
                Dispose();
                throw;
            }            
		}

		public string[] Query(string query, string[] sortFields, int start, int pageSize, out int totalResults)
		{
			IndexSearcher searcher;
			using (GetSearcher(out searcher))
			{
				Query q;
				if (string.IsNullOrEmpty(query))
				{
					q = new MatchAllDocsQuery();
				}
				else
				{
					var queryParser = new RavenQueryParser(analyzer, NumericIndexFields);
                    q = queryParser.Parse(query);
				}

				var topDocs = ExecuteQuery(searcher, sortFields, q, pageSize + start);

				var results = new List<string>();

				for (var i = start; i < pageSize + start && i < topDocs.TotalHits; i++)
				{
					var document = searcher.Doc(topDocs.ScoreDocs[i].Doc);
					results.Add(document.Get("__key"));
				}
				totalResults = topDocs.TotalHits;
				return results.ToArray();
			}
		}

		private TopDocs ExecuteQuery(IndexSearcher searcher, string[] sortFields, Query q, int size)
		{
			TopDocs topDocs;
			if (sortFields != null && sortFields.Length > 0)
			{
				var sort = new Sort(sortFields.Select(field =>
				{
					var desc = field.StartsWith("-");
					if (desc)
						field = field.Substring(1);
					return new SortField(field, SortField.STRING, desc);
				}).ToArray());
				topDocs = searcher.Search(q, null, size, sort);
			}
			else
			{
				topDocs = searcher.Search(q, null, size);
			}
			return topDocs;
		}

        public virtual void Index(string key, RavenJObject metadata)
        {
            lock (writerLock)
            {
                var lowerKey = key.ToLowerInvariant();

                var doc = CreateDocument(lowerKey, metadata);

                // REVIEW: Check if there is more straight-forward/efficient pattern out there to work with RavenJObjects.
                var lookup = metadata.ToLookup(x => x.Key);
                foreach ( var metadataKey in lookup )
                {
                    foreach ( var metadataHolder in metadataKey )
                    {
                        var array = metadataHolder.Value as RavenJArray;
                        if (array != null)
                        {
                            // Object is an array. Therefore, we index each token. 
                            foreach (var item in array)
                                doc.Add(new Field(metadataHolder.Key, item.ToString(), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));                         
                        }
                        else doc.Add(new Field(metadataHolder.Key, metadataHolder.Value.ToString(), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
                    }
                }

                writer.DeleteDocuments(new Term("__key", lowerKey));
                writer.AddDocument(doc);
                // yes, this is slow, but we aren't expecting high writes count
                writer.Commit();
                ReplaceSearcher();
            }
        }

        private static Document CreateDocument(string lowerKey, RavenJObject metadata)
        {
            var doc = new Document();
            doc.Add(new Field("__key", lowerKey, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

            var fileName = Path.GetFileName(lowerKey);            
            Debug.Assert(fileName != null);
            doc.Add(new Field("__fileName", fileName, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            // the reversed version of the file name is used to allow searches that start with wildcards
            char[] revFileName = fileName.ToCharArray();
            Array.Reverse(revFileName);
            doc.Add(new Field("__rfileName", new string(revFileName), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));                        

            int level = 0;
            var directoryName = RavenFileNameHelper.RavenDirectory(Path.GetDirectoryName(lowerKey));


            doc.Add(new Field("__directory", directoryName, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            // the reversed version of the directory is used to allow searches that start with wildcards
            char[] revDirectory = directoryName.ToCharArray();
            Array.Reverse(revDirectory);
            doc.Add(new Field("__rdirectory", new string(revDirectory), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));

            do
            {
                level += 1;

                directoryName = RavenFileNameHelper.RavenDirectory(directoryName);

                doc.Add(new Field("__directoryName", directoryName, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
                // the reversed version of the directory is used to allow searches that start with wildcards
                char[] revDirectoryName = directoryName.ToCharArray();
                Array.Reverse(revDirectoryName);
                doc.Add(new Field("__rdirectoryName", new string(revDirectoryName), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));

                directoryName = Path.GetDirectoryName(directoryName);
            } 
            while (directoryName != null);

            doc.Add(new Field("__modified", DateTime.UtcNow.ToString(DateIndexFormat, CultureInfo.InvariantCulture), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            doc.Add(new Field("__level", level.ToString(CultureInfo.InvariantCulture), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            
            RavenJToken contentLen;
            if ( metadata.TryGetValue("Content-Length", out contentLen))
            {
                long len;
                if (long.TryParse(contentLen.Value<string>(), out len))
                {
                    doc.Add(new Field("__size", len.ToString("D20"), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
                    doc.Add(new NumericField("__size_numeric", Field.Store.NO, true).SetLongValue(len));
                }
            }

            return doc;
        }

		internal IDisposable GetSearcher(out IndexSearcher searcher)
		{
			return currentIndexSearcherHolder.GetSearcher(out searcher);
		}

		public void Dispose()
		{
			analyzer.Close();
			if (currentIndexSearcherHolder != null)
			{
				currentIndexSearcherHolder.SetIndexSearcher(null);
			}
			writer.Dispose();
			directory.Dispose();
		}

		public void Delete(string key)
		{
			var lowerKey = key.ToLowerInvariant();

			lock (writerLock)
			{
				writer.DeleteDocuments(new Term("__key", lowerKey));
				writer.Optimize();
				writer.Commit();
				ReplaceSearcher();
			}
		}

		private void ReplaceSearcher()
		{
			currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(writer.GetReader()));
		}

		public IEnumerable<string> GetTermsFor(string field, string fromValue)
		{
			IndexSearcher searcher;
			using (GetSearcher(out searcher))
			{
				var termEnum = searcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty));
				try
				{
					if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
					{
						while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
						{
							if (termEnum.Next() == false)
								yield break;
						}
					}
					while (termEnum.Term == null ||
						field.Equals(termEnum.Term.Field))
					{
						if (termEnum.Term != null)
						{
							var item = termEnum.Term.Text;
							yield return item;
						}

						if (termEnum.Next() == false)
							break;
					}
				}
				finally
				{
					termEnum.Dispose();
				}
			}
		}

        public void Backup(string backupDirectory)
	    {
            bool hasSnapshot = false;
            bool throwOnFinallyException = true;
            try
            {
                var existingFiles = new HashSet<string>();
                var allFilesPath = Path.Combine(backupDirectory, "index-files.all-existing-index-files");
                var saveToFolder = Path.Combine(backupDirectory, "Indexes");
                System.IO.Directory.CreateDirectory(saveToFolder);
                if (File.Exists(allFilesPath))
                {
                    foreach (var file in File.ReadLines(allFilesPath))
                    {
                        existingFiles.Add(file);
                    }
                }

                var neededFilePath = Path.Combine(saveToFolder, "index-files.required-for-index-restore");
                using (var allFilesWriter = File.Exists(allFilesPath) ? File.AppendText(allFilesPath) : File.CreateText(allFilesPath))
                using (var neededFilesWriter = File.CreateText(neededFilePath))
                {
                    var segmentsFileName = "segments.gen";
                    var segmentsFullPath = Path.Combine(path, segmentsFileName);
                    File.Copy(segmentsFullPath, Path.Combine(saveToFolder, segmentsFileName));
                    allFilesWriter.WriteLine(segmentsFileName);
                    neededFilesWriter.WriteLine(segmentsFileName);

                    var commit = snapshotter.Snapshot();
                    hasSnapshot = true;
                    foreach (var fileName in commit.FileNames)
                    {
                        var fullPath = Path.Combine(path, fileName);

                        if (".lock".Equals(Path.GetExtension(fullPath), StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (File.Exists(fullPath) == false)
                            continue;

                        if (existingFiles.Contains(fileName) == false)
                        {
                            var destFileName = Path.Combine(saveToFolder, fileName);
                            try
                            {
                                File.Copy(fullPath, destFileName);
                            }
                            catch (Exception e)
                            {
                                log.WarnException(
                                    "Could not backup RavenFS index" + 
                                    " because failed to copy file : " + fullPath + ". Skipping the index, will force index reset on restore", e); //TODO: is it also true for RavenFS?
                                neededFilesWriter.Dispose();
                                TryDelete(neededFilePath);
                                return;

                            }
                            allFilesWriter.WriteLine(fileName);
                        }
                        neededFilesWriter.WriteLine(fileName);
                    }
                    allFilesWriter.Flush();
                    neededFilesWriter.Flush();
                }
            }
            catch
            {
                throwOnFinallyException = false;
                throw;
            }
            finally
            {
                if (snapshotter != null && hasSnapshot)
                {
                    try
                    {
                        snapshotter.Release();
                    }
                    catch
                    {
                        if (throwOnFinallyException)
                            throw;
                    }
                }
            }
	    }

        private static void TryDelete(string neededFilePath)
        {
            try
            {
                File.Delete(neededFilePath);
            }
            catch (Exception)
            {
            }
        }
	}
}