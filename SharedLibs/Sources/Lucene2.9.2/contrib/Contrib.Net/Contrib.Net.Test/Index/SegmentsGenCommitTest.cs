/**
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
using System.Text;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Test.Index
{
    /// <summary>
    /// Summary description for SegmentsGenCommitTest
    /// </summary>
    [TestFixture]
    public class SegmentsGenCommitTest
    {
        public SegmentsGenCommitTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        private RAMDirectory directory;

        #region setup/teardown methods

        [SetUp]
        public void MyTestInitialize()
        {
            this.directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(this.directory, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            for (int i = 0; i < 10; i++)
            {
                Document d = new Document();
                d.Add(new Field("Text", i.ToString(), Field.Store.YES, Field.Index.ANALYZED));
                writer.AddDocument(d);
                writer.Commit();
            }
            writer.Close();
        }

        [TearDown]
        public void MyTestCleanup()
        {
            if (this.directory != null)
            {
                this.directory.Close();
                this.directory = null;
            }
        }

        #endregion

        [Test]
        public void ReadSegmentsGenTest()
        {
            // check the generation in the directory
            IndexReader reader = IndexReader.Open(this.directory, true);
            IndexCommit commit = reader.GetIndexCommit();

            // create a SegmentsGenCommit
            SegmentsGenCommit sgCommit = new SegmentsGenCommit(this.directory);

            Assert.AreEqual(commit.GetGeneration(), sgCommit.GetGeneration());
            Assert.AreEqual(commit.GetSegmentsFileName(), sgCommit.GetSegmentsFileName());
        }

        [Test]
        public void OpenWriterWithCommit()
        {
            SegmentsGenCommit sgCommit = new SegmentsGenCommit(this.directory);
            IndexWriter writer = new IndexWriter(this.directory, new WhitespaceAnalyzer(), null, IndexWriter.MaxFieldLength.UNLIMITED, sgCommit);
            Assert.AreEqual(10, writer.MaxDoc());
            IndexReader reader = writer.GetReader();

            IndexCommit commit = reader.GetIndexCommit();
            Assert.AreEqual(commit.GetGeneration(), sgCommit.GetGeneration());
            Assert.AreEqual(commit.GetSegmentsFileName(), sgCommit.GetSegmentsFileName());
        }
    }
}
