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
using System.Collections;
using System.Threading;

using NUnit.Framework;



namespace Lucene.Net._SupportClass
{
    [TestFixture]
    public class _SupportClassTestCases
    {
        [Test]
        public void Count()
        {
            System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();
            Type[] types = asm.GetTypes();

            int countSupport = 0;
            int countOther = 0;
            foreach (Type type in types)
            {
                object[] o1 = type.GetCustomAttributes(typeof(NUnit.Framework.TestFixtureAttribute), true);
                if (o1 == null || o1.Length == 0) continue;

                foreach (System.Reflection.MethodInfo mi in type.GetMethods())
                {
                    object[] o2 = mi.GetCustomAttributes(typeof(NUnit.Framework.TestAttribute), true);
                    if (o2 == null || o2.Length == 0) continue;

                    if (type.FullName.StartsWith("Lucene.Net._SupportClass"))
                    {
                        countSupport++;
                    }
                    else
                    {
                        countOther++;
                    }
                }
            }
            string msg = "Lucene.Net TestCases:" + countSupport + "     Other TestCases:" + countOther;
            Console.WriteLine(msg);
            Assert.Ignore("[Intentionally ignored test case] " + msg);
        }
    }

    /// <summary>
    /// </summary>
    [TestFixture]
    public class TestSupportClass
    {
        /// <summary></summary>
        /// <throws></throws>
        [Test]
        public virtual void TestCRC32()
        {
            byte[] b = new byte[256];
            for (int i = 0; i < b.Length; i++)
                b[i] = (byte)i;

            SupportClass.Checksum digest = new SupportClass.CRC32();
            digest.Update(b, 0, b.Length);

            Int64 expected = 688229491;
            Assert.AreEqual(expected, digest.GetValue());
        }
    }

    [TestFixture]
    public class TestWeakHashTable
    {
        [Test]
        public void A_TestBasicOps()
        {
            IDictionary weakHashTable = TestWeakHashTableBehavior.CreateDictionary();// new SupportClass.TjWeakHashTable();
            Hashtable realHashTable = new Hashtable();

            SmallObject[] so = new SmallObject[100];
            for (int i = 0; i < 20000; i++)
            {
                SmallObject key = new SmallObject(i);
                SmallObject value = key;
                so[i / 200] = key;
                realHashTable.Add(key, value);
                weakHashTable.Add(key, value);
            }

            Assert.AreEqual(weakHashTable.Count, realHashTable.Count);

            ICollection keys = (ICollection)realHashTable.Keys;

            foreach (SmallObject key in keys)
            {
                Assert.AreEqual(((SmallObject)realHashTable[key]).i,
                                ((SmallObject)weakHashTable[key]).i);

                Assert.IsTrue(realHashTable[key].Equals(weakHashTable[key]));
            }


            ICollection values1 = (ICollection)weakHashTable.Values;
            ICollection values2 = (ICollection)realHashTable.Values;
            Assert.AreEqual(values1.Count, values2.Count);

            realHashTable.Remove(new SmallObject(10000));
            weakHashTable.Remove(new SmallObject(10000));
            Assert.AreEqual(weakHashTable.Count, 20000);
            Assert.AreEqual(realHashTable.Count, 20000);

            for (int i = 0; i < so.Length; i++)
            {
                realHashTable.Remove(so[i]);
                weakHashTable.Remove(so[i]);
                Assert.AreEqual(weakHashTable.Count, 20000 - i - 1);
                Assert.AreEqual(realHashTable.Count, 20000 - i - 1);
            }

            //After removals, compare the collections again.
            ICollection keys2 = (ICollection)realHashTable.Keys;
            foreach (SmallObject o in keys2)
            {
                Assert.AreEqual(((SmallObject)realHashTable[o]).i,
                                ((SmallObject)weakHashTable[o]).i);
                Assert.IsTrue(realHashTable[o].Equals(weakHashTable[o]));
            }
        }

        [Test]
        public void B_TestOutOfMemory()
        {
            IDictionary wht = TestWeakHashTableBehavior.CreateDictionary();
            int OOMECount = 0;

            for (int i = 0; i < 1024 * 24 + 32; i++) // total requested Mem. > 24GB
            {
                try
                {
                    wht.Add(new BigObject(i), i);
                    if (i % 1024 == 0) Console.WriteLine("Requested Mem: " + i.ToString() + " MB");
                    OOMECount = 0;
                }
                catch (OutOfMemoryException oom)
                {
                    if (OOMECount++ > 10) throw new Exception("Memory Allocation Error in B_TestOutOfMemory");
                    //Try Again. GC will eventually release some memory.
                    Console.WriteLine("OOME WHEN i=" + i.ToString() + ". Try Again");
                    System.Threading.Thread.Sleep(10);
                    i--;
                    continue;
                }
            }

            GC.Collect();
            Console.WriteLine("Passed out of memory exception.");
        }

        private int GetMemUsageInKB()
        {
            return System.Diagnostics.Process.GetCurrentProcess().WorkingSet / 1024;
        }

        [Test]
        public void C_TestMemLeakage()
        {

            IDictionary wht = TestWeakHashTableBehavior.CreateDictionary(); //new SupportClass.TjWeakHashTable();

            GC.Collect();
            int initialMemUsage = GetMemUsageInKB();

            Console.WriteLine("Initial MemUsage=" + initialMemUsage);
            for (int i = 0; i < 10000; i++)
            {
                wht.Add(new BigObject(i), i);
                if (i % 100 == 0)
                {
                    int mu = GetMemUsageInKB();
                    Console.WriteLine(i.ToString() + ") MemUsage=" + mu);
                }
            }

            GC.Collect();
            int memUsage = GetMemUsageInKB();
            if (memUsage > initialMemUsage * 2) Assert.Fail("Memory Leakage.MemUsage = " + memUsage);
        }
    }

    [TestFixture]
    public class TestWeakHashTableBehavior
    {
        IDictionary dictionary;

        public static IDictionary CreateDictionary()
        {
            return new SupportClass.WeakHashTable();
        }


        private void CallGC()
        {
            for (int i = 0; i < 10; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        [SetUp]
        public void Setup()
        {
            dictionary = CreateDictionary();
        }

        [Test]
        public void Test_Dictionary_Add()
        {
            string key = "A";

            dictionary.Add(key, "value");
            Assert.IsTrue(dictionary.Contains(key));
            Assert.AreEqual("value", dictionary[key]);
            Assert.AreEqual(1, dictionary.Count);

            CollectionAssert.AreEquivalent(dictionary.Values, new object[] { "value" });
        }

        [Test]
        public void Test_Dictionary_Add_2()
        {
            string key = "A";
            string key2 = "B";

            dictionary.Add(key, "value");
            dictionary.Add(key2, "value2");
            Assert.IsTrue(dictionary.Contains(key));
            Assert.IsTrue(dictionary.Contains(key2));
            Assert.AreEqual("value", dictionary[key]);
            Assert.AreEqual("value2", dictionary[key2]);
            Assert.AreEqual(2, dictionary.Count);

            CollectionAssert.AreEquivalent(dictionary.Values, new object[] { "value", "value2" });
        }

        [Test]
        public void Test_Keys()
        {
            string key = "A";
            string key2 = "B";

            dictionary.Add(key, "value");
            CollectionAssert.AreEquivalent(dictionary.Keys, new object[] { key });

            dictionary.Add(key2, "value2");
            CollectionAssert.AreEquivalent(dictionary.Keys, new object[] { key, key2 });
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Test_Dictionary_Add_Null()
        {
            dictionary.Add(null, "value");
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Test_Dictionary_Set_Null()
        {
            dictionary[null] = "value";
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void Test_Dictionary_AddTwice()
        {
            string key = "A";

            dictionary.Add(key, "value");
            dictionary.Add(key, "value2");
        }

        [Test]
        public void Test_Dictionary_AddReplace()
        {
            string key = "A";
            string key2 = "a".ToUpper();

            dictionary.Add(key, "value");
            dictionary[key2] = "value2";

            Assert.AreEqual(1, dictionary.Count);
            Assert.IsTrue(dictionary.Contains(key));
            Assert.AreEqual("value2", dictionary[key]);
        }

        [Test]
        public void Test_Dictionary_AddRemove()
        {
            string key = "A";

            dictionary.Add(key, "value");
            dictionary.Remove(key);

            Assert.AreEqual(0, dictionary.Count);
            Assert.IsFalse(dictionary.Contains(key));
            Assert.IsNull(dictionary[key]);
        }

        [Test]
        public void Test_Dictionary_Clear()
        {
            string key = "A";

            dictionary.Add(key, "value");
            dictionary.Clear();

            Assert.AreEqual(0, dictionary.Count);
            Assert.IsFalse(dictionary.Contains(key));
            Assert.IsNull(dictionary[key]);
        }

        [Test]
        public void Test_Dictionary_AddRemove_2()
        {
            string key = "A";

            dictionary.Add(key, "value");
            dictionary.Remove(key);
            dictionary.Remove(key);

            Assert.AreEqual(0, dictionary.Count);
            Assert.IsFalse(dictionary.Contains(key));
            Assert.IsNull(dictionary[key]);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Test_Dictionary_Get_Null()
        {
            object value = dictionary[null];
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Test_Dictionary_Remove_Null()
        {
            dictionary.Remove(null);
        }

        [Test]
        public void Test_Dictionary_CopyTo()
        {
            string key = "A";

            dictionary.Add(key, "value");
            DictionaryEntry[] a = new DictionaryEntry[1];
            dictionary.CopyTo(a, 0);

            DictionaryEntry de = (DictionaryEntry)a[0];
            Assert.AreEqual(key, de.Key);
            Assert.AreEqual("value", de.Value);
        }

        [Test]
        public void Test_Dictionary_GetEnumerator()
        {
            string key = "A";

            dictionary.Add(key, "value");

            IDictionaryEnumerator de = dictionary.GetEnumerator();
            Assert.IsTrue(de.MoveNext());
            Assert.AreEqual(key, de.Key);
            Assert.AreEqual("value", de.Value);
        }

        [Test]
        public void Test_Dictionary_ForEach()
        {
            string key = "A";

            dictionary.Add(key, "value");

            IEnumerable enumerable = dictionary;

            foreach (DictionaryEntry de in enumerable)
            {
                Assert.AreEqual(key, de.Key);
                Assert.AreEqual("value", de.Value);
            }
        }

        [Test]
        public void Test_Collisions()
        {
            //Create 2 keys with same hashcode but that are not equal
            CollisionTester key1 = new CollisionTester(1, 100);
            CollisionTester key2 = new CollisionTester(2, 100);

            dictionary.Add(key1, "value1");
            dictionary.Add(key2, "value2");

            Assert.AreEqual("value1", dictionary[key1]);
            Assert.AreEqual("value2", dictionary[key2]);

            dictionary.Remove(key1);
            Assert.AreEqual(null, dictionary[key1]);
        }

        [Test]
        public void Test_Weak_1()
        {
            BigObject key = new BigObject(1);
            BigObject key2 = new BigObject(2);

            dictionary.Add(key, "value");
            Assert.AreEqual("value", dictionary[key]);

            key = null;
            CallGC();

            dictionary.Add(key2, "value2");
            Assert.AreEqual(1, dictionary.Count);
        }

        [Test]
        public void Test_Weak_2()
        {
            BigObject key = new BigObject(1);
            BigObject key2 = new BigObject(2);
            BigObject key3 = new BigObject(3);

            dictionary.Add(key, "value");
            dictionary.Add(key2, "value2");
            Assert.AreEqual("value", dictionary[key]);

            key = null;
            CallGC();

            dictionary.Add(key3, "value3");

            Assert.AreEqual(2, dictionary.Count);
            Assert.IsNotNull(key2);
        }

        [Test]
        public void Test_Weak_ForEach()
        {
            BigObject[] keys1 = new BigObject[20];
            BigObject[] keys2 = new BigObject[20];

            for (int i = 0; i < keys1.Length; i++)
            {
                keys1[i] = new BigObject(i);
                dictionary.Add(keys1[i], "value");
            }
            for (int i = 0; i < keys2.Length; i++)
            {
                keys2[i] = new BigObject(i);
                dictionary.Add(keys2[i], "value");
            }

            Assert.AreEqual(40, dictionary.Count);

            keys2 = null;
            int count = 0;
            foreach (DictionaryEntry de in dictionary)
            {
                CallGC();
                count++;
            }

            Assert.LessOrEqual(20, count);
            Assert.Greater(40, count);
            Assert.IsNotNull(keys1);
        }
    }

    [TestFixture]
    public class TestWeakHashTableMultiThreadAccess
    {
        SupportClass.WeakHashTable wht = new SupportClass.WeakHashTable();
        Exception AnyException = null;
        bool EndOfTest = false;

        [Test]
        public void Test()
        {
            CreateThread(Add);
            CreateThread(Enum);
            
            int count = 200;
            while (count-- > 0)
            {
                Thread.Sleep(50);
                if (AnyException != null)
                {
                    EndOfTest = true;
                    Thread.Sleep(50);
                    Assert.Fail(AnyException.Message);
                }
            }
        }

        void CreateThread(ThreadStart fxn)
        {
            Thread t = new Thread(fxn);
            t.IsBackground = true;
            t.Start();
        }
        

        void Add()
        {
            try
            {
                long count = 0;
                while (EndOfTest==false)
                {
                    wht.Add(count.ToString(), count.ToString());
                    Thread.Sleep(1);

                    string toReplace = (count - 10).ToString();
                    if (wht.Contains(toReplace))
                    {
                        wht[toReplace] = "aa";
                    }

                    count++;
                }
            }
            catch (Exception ex)
            {
                AnyException = ex;
            }
        }

        void Enum()
        {
            try
            {
                while (EndOfTest==false)
                {
                    System.Collections.IEnumerator e = wht.Keys.GetEnumerator();
                    while (e.MoveNext())
                    {
                        string s = (string)e.Current;
                    }
                    Thread.Sleep(1);
                }
            }
            catch (Exception ex)
            {
                AnyException = ex;
            }
        }
    }

    class CollisionTester
    {
        int id;
        int hashCode;

        public CollisionTester(int id, int hashCode)
        {
            this.id = id;
            this.hashCode = hashCode;
        }

        public override int GetHashCode()
        {
            return hashCode;
        }

        public override bool Equals(object obj)
        {
            if (obj is CollisionTester)
            {
                return this.id == ((CollisionTester)obj).id;
            }
            else
                return base.Equals(obj);
        }
    }

    [TestFixture]
    public class TestWeakHashTablePerformance
    {
        IDictionary dictionary;
        SmallObject[] keys;


        [SetUp]
        public void Setup()
        {
            dictionary = TestWeakHashTableBehavior.CreateDictionary();
        }

        private void Fill(IDictionary dictionary)
        {
            foreach (SmallObject key in keys)
                dictionary.Add(key, "value");
        }

        [TestFixtureSetUp]
        public void TestSetup()
        {
            keys = new SmallObject[100000];
            for (int i = 0; i < keys.Length; i++)
                keys[i] = new SmallObject(i);
        }

        [Test]
        public void Test_Performance_Add()
        {
            for (int i = 0; i < 10; i++)
            {
                dictionary.Clear();
                Fill(dictionary);
            }
        }

        [Test]
        public void Test_Performance_Remove()
        {
            for (int i = 0; i < 10; i++)
            {
                Fill(dictionary);
                foreach (SmallObject key in keys)
                    dictionary.Remove(key);
            }
        }

        [Test]
        public void Test_Performance_Replace()
        {
            for (int i = 0; i < 10; i++)
            {
                foreach (SmallObject key in keys)
                    dictionary[key] = "value2";
            }
        }

        [Test]
        public void Test_Performance_Access()
        {
            Fill(dictionary);
            for (int i = 0; i < 10; i++)
            {
                foreach (SmallObject key in keys)
                {
                    object value = dictionary[key];
                }
            }
        }

        [Test]
        public void Test_Performance_Contains()
        {
            Fill(dictionary);
            for (int i = 0; i < 10; i++)
            {
                foreach (SmallObject key in keys)
                {
                    dictionary.Contains(key);
                }
            }
        }

        [Test]
        public void Test_Performance_Keys()
        {
            Fill(dictionary);
            for (int i = 0; i < 100; i++)
            {
                ICollection keys = dictionary.Keys;
            }
        }

        [Test]
        public void Test_Performance_ForEach()
        {
            Fill(dictionary);
            for (int i = 0; i < 10; i++)
            {
                foreach (DictionaryEntry de in dictionary)
                {

                }
            }
        }

        [Test]
        public void Test_Performance_CopyTo()
        {
            Fill(dictionary);
            DictionaryEntry[] array = new DictionaryEntry[dictionary.Count];

            for (int i = 0; i < 10; i++)
            {
                dictionary.CopyTo(array, 0);
            }
        }
    }

    internal class BigObject
    {
        public int i = 0;
        public byte[] buf = null;

        public BigObject(int i)
        {
            this.i = i;
            buf = new byte[1024 * 1024]; //1MB
        }
    }


    internal class SmallObject
    {
        public int i = 0;

        public SmallObject(int i)
        {
            this.i = i;
        }
    }

    [TestFixture]
    public class TestThreadClass
    {
        [Test]
        public void Test()
        {
            SupportClass.ThreadClass thread = new SupportClass.ThreadClass();

            //Compare Current Thread Ids
            Assert.IsTrue(SupportClass.ThreadClass.Current().Instance.ManagedThreadId == System.Threading.Thread.CurrentThread.ManagedThreadId);


            //Compare instances of ThreadClass
            MyThread mythread = new MyThread();
            mythread.Start();
            while (mythread.Result == null) System.Threading.Thread.Sleep(1);
            Assert.IsTrue((bool)mythread.Result);


            SupportClass.ThreadClass nullThread = null;
            Assert.IsTrue(nullThread == null); //test overloaded operator == with null values
            Assert.IsFalse(nullThread != null); //test overloaded operator != with null values
        }

        class MyThread : SupportClass.ThreadClass
        {
            public object Result = null;
            public override void Run()
            {
                Result = SupportClass.ThreadClass.Current() == this;
            }
        }
    }

    [TestFixture]
    public class TestOSClass
    {
        // LUCENENET-216
        [Test]
        public void TestFSDirectorySync()
        {
            System.IO.FileInfo path = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testsync"));
            Lucene.Net.Store.Directory directory = new Lucene.Net.Store.SimpleFSDirectory(path, null);
            try
            {
                Lucene.Net.Store.IndexOutput io = directory.CreateOutput("syncfile");
                io.Close();
                directory.Sync("syncfile");
            }
            finally
            {
                directory.Close();
                Lucene.Net.Util._TestUtil.RmDir(path);
            }
        }
    }

    [TestFixture]
    public class TestLRUCache
    {
        [Test]
        public void Test()
        {
            Lucene.Net.Util.Cache.SimpleLRUCache cache = new Lucene.Net.Util.Cache.SimpleLRUCache(3);
            cache.Put("a", "a");
            cache.Put("b", "b");
            cache.Put("c", "c");
            Assert.IsNotNull(cache.Get("a"));
            Assert.IsNotNull(cache.Get("b"));
            Assert.IsNotNull(cache.Get("c"));
            cache.Put("d", "d");
            Assert.IsNull(cache.Get("a"));
            Assert.IsNotNull(cache.Get("c"));
            cache.Put("e", "e");
            cache.Put("f", "f");
            Assert.IsNotNull(cache.Get("c"));
        }
    }

    [TestFixture]
    public class TestOldPatches
    {
        //-------------------------------------------
        [Test]
        [Description("LUCENENET-183")]
        public void Test_SegmentTermVector_IndexOf()
        {
            Lucene.Net.Store.RAMDirectory directory = new Lucene.Net.Store.RAMDirectory();
            Lucene.Net.Analysis.Analyzer analyzer = new Lucene.Net.Analysis.WhitespaceAnalyzer();
            Lucene.Net.Index.IndexWriter writer = new Lucene.Net.Index.IndexWriter(directory, analyzer, Lucene.Net.Index.IndexWriter.MaxFieldLength.LIMITED);
            Lucene.Net.Documents.Document document = new Lucene.Net.Documents.Document();
            document.Add(new Lucene.Net.Documents.Field("contents", new System.IO.StreamReader(new System.IO.MemoryStream(System.Text.Encoding.ASCII.GetBytes("a_ a0"))), Lucene.Net.Documents.Field.TermVector.WITH_OFFSETS));
            writer.AddDocument(document);
            Lucene.Net.Index.IndexReader reader = writer.GetReader();
            Lucene.Net.Index.TermPositionVector tpv = reader.GetTermFreqVector(0, "contents") as Lucene.Net.Index.TermPositionVector;
            //Console.WriteLine("tpv: " + tpv);
            int index = tpv.IndexOf("a_");
            Assert.AreEqual(index, 1, "See the issue: LUCENENET-183");
        }

        //-------------------------------------------
        [Test]
        [Description("LUCENENET-170")]
        public void Test_Util_Parameter()
        {
            Lucene.Net.Search.BooleanQuery queryPreSerialized = new Lucene.Net.Search.BooleanQuery();
            queryPreSerialized.Add(new Lucene.Net.Search.TermQuery(new Lucene.Net.Index.Term("country", "Russia")), Lucene.Net.Search.BooleanClause.Occur.MUST);
            queryPreSerialized.Add(new Lucene.Net.Search.TermQuery(new Lucene.Net.Index.Term("country", "France")), Lucene.Net.Search.BooleanClause.Occur.MUST);

            //now serialize it 
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter serializer = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            System.IO.MemoryStream memoryStream = new System.IO.MemoryStream();
            serializer.Serialize(memoryStream, queryPreSerialized);

            //now deserialize 
            memoryStream.Seek(0, System.IO.SeekOrigin.Begin);
            Lucene.Net.Search.BooleanQuery queryPostSerialized = (Lucene.Net.Search.BooleanQuery)serializer.Deserialize(memoryStream);

            memoryStream.Close();

            Assert.AreEqual(queryPreSerialized, queryPostSerialized, "See the issue: LUCENENET-170");
        }

        //-------------------------------------------
        [Test]
        [Description("LUCENENET-174")]
        public void Test_Store_RAMDirectory()
        {
            Lucene.Net.Store.RAMDirectory ramDIR = new Lucene.Net.Store.RAMDirectory();

            //Index 1 Doc
            Lucene.Net.Index.IndexWriter wr = new Lucene.Net.Index.IndexWriter(ramDIR, new Lucene.Net.Analysis.WhitespaceAnalyzer(), true);
            Lucene.Net.Documents.Document doc = new Lucene.Net.Documents.Document();
            doc.Add(new Lucene.Net.Documents.Field("field1", "value1 value11", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.TOKENIZED));
            wr.AddDocument(doc);
            wr.Close();

            //now serialize it 
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter serializer = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            System.IO.MemoryStream memoryStream = new System.IO.MemoryStream();
            serializer.Serialize(memoryStream, ramDIR);

            //Close DIR
            ramDIR.Close();
            ramDIR = null;

            //now deserialize 
            memoryStream.Seek(0, System.IO.SeekOrigin.Begin);
            Lucene.Net.Store.RAMDirectory ramDIR2 = (Lucene.Net.Store.RAMDirectory)serializer.Deserialize(memoryStream);

            //Add 1 more doc
            wr = new Lucene.Net.Index.IndexWriter(ramDIR2, new Lucene.Net.Analysis.WhitespaceAnalyzer(), false);
            doc = new Lucene.Net.Documents.Document();
            doc.Add(new Lucene.Net.Documents.Field("field1", "value1 value11", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.TOKENIZED));
            wr.AddDocument(doc);
            wr.Close();

            //Search
            Lucene.Net.Search.IndexSearcher s = new Lucene.Net.Search.IndexSearcher(ramDIR2);
            Lucene.Net.QueryParsers.QueryParser qp = new Lucene.Net.QueryParsers.QueryParser("field1", new Lucene.Net.Analysis.Standard.StandardAnalyzer());
            Lucene.Net.Search.Query q = qp.Parse("value1");
            Lucene.Net.Search.TopDocs topDocs = s.Search(q, 100);
            s.Close();

            Assert.AreEqual(topDocs.totalHits, 2, "See the issue: LUCENENET-174");
        }



        //-------------------------------------------
        [Test]
        [Description("LUCENENET-150")]
        public void Test_Index_ReusableStringReader()
        {
            Lucene.Net.Index.IndexWriter wr = new Lucene.Net.Index.IndexWriter(new Lucene.Net.Store.RAMDirectory(), new TestAnalyzer(), true);

            Lucene.Net.Documents.Document doc = new Lucene.Net.Documents.Document();
            Lucene.Net.Documents.Field f1 = new Lucene.Net.Documents.Field("f1", TEST_STRING, Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.TOKENIZED);
            doc.Add(f1);
            wr.AddDocument(doc);

            wr.Close();
        }

        static string TEST_STRING = "Some Text and some more Text";

        class TestAnalyzer : Lucene.Net.Analysis.Analyzer
        {
            public override Lucene.Net.Analysis.TokenStream TokenStream(string fieldName, System.IO.TextReader reader)
            {
                return new TestTokenizer(reader);
            }
        }

        class TestTokenizer : Lucene.Net.Analysis.CharTokenizer
        {
            public TestTokenizer(System.IO.TextReader Reader)
                : base(Reader)
            {
                //Caution: "Reader" is actually of type "ReusableStringReader" and some 
                //methods (for ex. "ReadToEnd", "Peek",  "ReadLine") is not implemented. 
                Assert.AreEqual(TEST_STRING, Reader.ReadToEnd(), "Issue LUCENENET-150: \"ReadToEnd\" method is not implemented");
            }

            protected override bool IsTokenChar(char c)
            {
                return char.IsLetterOrDigit(c);
            }
        }


        //-------------------------------------------
        [Test]
        [Description("LUCENENET-100")]
        public void Test_Search_FieldDoc()
        {
            try
            {
                LUCENENET_100_CreateIndex();

                System.Runtime.Remoting.Channels.ChannelServices.RegisterChannel(new System.Runtime.Remoting.Channels.Tcp.TcpChannel(38087));

                Lucene.Net.Search.IndexSearcher indexSearcher = new Lucene.Net.Search.IndexSearcher(LUCENENET_100_Dir);
                System.Runtime.Remoting.RemotingServices.Marshal(indexSearcher, "Searcher");

                LUCENENET_100_ClientSearch();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            //Wait Client to finish
            while (LUCENENET_100_testFinished == false) System.Threading.Thread.Sleep(10);

            if (LUCENENET_100_Exception != null) throw LUCENENET_100_Exception;

        }

        Lucene.Net.Store.RAMDirectory LUCENENET_100_Dir = new Lucene.Net.Store.RAMDirectory();
        bool LUCENENET_100_testFinished = false;
        Exception LUCENENET_100_Exception = null;


        void LUCENENET_100_ClientSearch()
        {
            try
            {
                Lucene.Net.Search.Searchable s = (Lucene.Net.Search.Searchable)Activator.GetObject(typeof(Lucene.Net.Search.Searchable), @"tcp://localhost:38087/Searcher");
                Lucene.Net.Search.MultiSearcher searcher = new Lucene.Net.Search.MultiSearcher(new Lucene.Net.Search.Searchable[] { s });

                Lucene.Net.Search.Query q = new Lucene.Net.Search.TermQuery(new Lucene.Net.Index.Term("field1", "moon"));

                Lucene.Net.Search.Sort sort = new Lucene.Net.Search.Sort();
                sort.SetSort(new Lucene.Net.Search.SortField("field2", Lucene.Net.Search.SortField.INT));

                Lucene.Net.Search.TopDocs h = searcher.Search(q, null,100, sort);
            }
            catch (Exception ex)
            {
                LUCENENET_100_Exception = ex;
            }
            finally
            {
                LUCENENET_100_testFinished = true;
            }
        }

        void LUCENENET_100_CreateIndex()
        {
            Lucene.Net.Index.IndexWriter w = new Lucene.Net.Index.IndexWriter(LUCENENET_100_Dir, new Lucene.Net.Analysis.Standard.StandardAnalyzer(), true);

            Lucene.Net.Documents.Field f1 = new Lucene.Net.Documents.Field("field1", "dark side of the moon", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.TOKENIZED);
            Lucene.Net.Documents.Field f2 = new Lucene.Net.Documents.Field("field2", "123", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.UN_TOKENIZED);
            Lucene.Net.Documents.Document d = new Lucene.Net.Documents.Document();
            d.Add(f1);
            d.Add(f2);
            w.AddDocument(d);

            f1 = new Lucene.Net.Documents.Field("field1", "Fly me to the moon", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.TOKENIZED);
            f2 = new Lucene.Net.Documents.Field("field2", "456", Lucene.Net.Documents.Field.Store.YES, Lucene.Net.Documents.Field.Index.UN_TOKENIZED);
            d = new Lucene.Net.Documents.Document();
            d.Add(f1);
            d.Add(f2);
            w.AddDocument(d);

            w.Close();
        }

        //-------------------------------------------
    }

    [TestFixture]
    public class TestSerialization
    {
        Lucene.Net.Store.RAMDirectory dir = null;

        [SetUp]
        public void Setup()
        {
            dir = new Lucene.Net.Store.RAMDirectory();
            Index();
        }

        void Index()
        {
            Lucene.Net.Index.IndexWriter wr = new Lucene.Net.Index.IndexWriter(dir, new Lucene.Net.Analysis.WhitespaceAnalyzer(), Lucene.Net.Index.IndexWriter.MaxFieldLength.UNLIMITED);

            Lucene.Net.Documents.Document doc = null;
            Lucene.Net.Documents.Field f = null;

            doc = new Lucene.Net.Documents.Document();
            f = new Lucene.Net.Documents.Field("field", "a b c d", Lucene.Net.Documents.Field.Store.NO, Lucene.Net.Documents.Field.Index.ANALYZED);
            doc.Add(f);
            wr.AddDocument(doc);

            doc = new Lucene.Net.Documents.Document();
            f = new Lucene.Net.Documents.Field("field", "a b a d", Lucene.Net.Documents.Field.Store.NO, Lucene.Net.Documents.Field.Index.ANALYZED);
            doc.Add(f);
            wr.AddDocument(doc);

            doc = new Lucene.Net.Documents.Document();
            f = new Lucene.Net.Documents.Field("field", "a b e f", Lucene.Net.Documents.Field.Store.NO, Lucene.Net.Documents.Field.Index.ANALYZED);
            doc.Add(f);
            wr.AddDocument(doc);
            
            doc = new Lucene.Net.Documents.Document();
            f = new Lucene.Net.Documents.Field("field", "x y z", Lucene.Net.Documents.Field.Store.NO, Lucene.Net.Documents.Field.Index.ANALYZED);
            doc.Add(f);
            wr.AddDocument(doc);
            
            wr.Close();
        }


        [Test]
        [Description("LUCENENET-338  (also see LUCENENET-170)")]
        public void TestBooleanQuerySerialization()
        {
            Lucene.Net.Search.BooleanQuery lucQuery = new Lucene.Net.Search.BooleanQuery();

            lucQuery.Add(new Lucene.Net.Search.TermQuery(new Lucene.Net.Index.Term("field", "x")), Lucene.Net.Search.BooleanClause.Occur.MUST);
            
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bf = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            System.IO.MemoryStream ms = new System.IO.MemoryStream();
            bf.Serialize(ms, lucQuery);
            ms.Seek(0, System.IO.SeekOrigin.Begin);
            Lucene.Net.Search.BooleanQuery lucQuery2 = (Lucene.Net.Search.BooleanQuery)bf.Deserialize(ms);
            ms.Close();

            Assert.AreEqual(lucQuery, lucQuery2, "Error in serialization");

            Lucene.Net.Search.IndexSearcher searcher = new Lucene.Net.Search.IndexSearcher(dir, true);

            int hitCount = searcher.Search(lucQuery, 20).totalHits;
            
            searcher.Close();
            searcher = new Lucene.Net.Search.IndexSearcher(dir, true);

            int hitCount2 = searcher.Search(lucQuery2, 20).totalHits;

            Assert.AreEqual(hitCount, hitCount2,"Error in serialization - different hit counts");
        }
    }

}


namespace Lucene.Net
{
    /// <summary>
    /// Support for junit.framework.TestCase.getName().
    /// {{Lucene.Net-2.9.1}} Move to another location after LUCENENET-266
    /// </summary>
    public class TestCase
    {
        public static string GetName()
        {
            return GetTestCaseName(false);
        }

        public static string GetFullName()
        {
            return GetTestCaseName(true);
        }

        static string GetTestCaseName(bool fullName)
        {
            System.Diagnostics.StackTrace stackTrace = new System.Diagnostics.StackTrace();
            for (int i = 0; i < stackTrace.FrameCount; i++)
            {
                System.Reflection.MethodBase method = stackTrace.GetFrame(i).GetMethod();
                object[] testAttrs = method.GetCustomAttributes(typeof(NUnit.Framework.TestAttribute), false);
                if (testAttrs != null && testAttrs.Length > 0)
                    if (fullName) return method.DeclaringType.FullName + "." + method.Name;
                    else return method.Name;
            }
            return "GetTestCaseName[UnknownTestMethod]";
        }
    }
}