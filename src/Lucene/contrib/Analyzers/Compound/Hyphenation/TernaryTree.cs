/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//using System;
//using System.Collections;
//using System.Text;

//namespace Lucene.Net.Analysis.Compound.Hyphenation
//{
//    /*
//     * <h2>Ternary Search Tree.</h2>
//     * 
//     * <p>
//     * A ternary search tree is a hybrid between a binary tree and a digital search
//     * tree (trie). Keys are limited to strings. A data value of type char is stored
//     * in each leaf node. It can be used as an index (or pointer) to the data.
//     * Branches that only contain one key are compressed to one node by storing a
//     * pointer to the trailer substring of the key. This class is intended to serve
//     * as base class or helper class to implement Dictionary collections or the
//     * like. Ternary trees have some nice properties as the following: the tree can
//     * be traversed in sorted order, partial matches (wildcard) can be implemented,
//     * retrieval of all keys within a given distance from the target, etc. The
//     * storage requirements are higher than a binary tree but a lot less than a
//     * trie. Performance is comparable with a hash table, sometimes it outperforms a
//     * hash function (most of the time can determine a miss faster than a hash).
//     * </p>
//     * 
//     * <p>
//     * The main purpose of this java port is to serve as a base for implementing
//     * TeX's hyphenation algorithm (see The TeXBook, appendix H). Each language
//     * requires from 5000 to 15000 hyphenation patterns which will be keys in this
//     * tree. The strings patterns are usually small (from 2 to 5 characters), but
//     * each char in the tree is stored in a node. Thus memory usage is the main
//     * concern. We will sacrifice 'elegance' to keep memory requirements to the
//     * minimum. Using java's char type as pointer (yes, I know pointer it is a
//     * forbidden word in java) we can keep the size of the node to be just 8 bytes
//     * (3 pointers and the data char). This gives room for about 65000 nodes. In my
//     * tests the english patterns took 7694 nodes and the german patterns 10055
//     * nodes, so I think we are safe.
//     * </p>
//     * 
//     * <p>
//     * All said, this is a map with strings as keys and char as value. Pretty
//     * limited!. It can be extended to a general map by using the string
//     * representation of an object and using the char value as an index to an array
//     * that contains the object values.
//     * </p>
//     * 
//     * This class has been taken from the Apache FOP project (http://xmlgraphics.apache.org/fop/). They have been slightly modified. 
//     */
//    [Serializable]
//    public class TernaryTree : ICloneable
//    {

//        /*
//         * We use 4 arrays to represent a node. I guess I should have created a proper
//         * node class, but somehow Knuth's pascal code made me forget we now have a
//         * portable language with virtual memory management and automatic garbage
//         * collection! And now is kind of late, furthermore, if it ain't broken, don't
//         * fix it.
//         */

//        /*
//         * Pointer to low branch and to rest of the key when it is stored directly in
//         * this node, we don't have unions in java!
//         */
//        protected char[] lo;

//        /*
//         * Pointer to high branch.
//         */
//        protected char[] hi;

//        /*
//         * Pointer to equal branch and to data when this node is a string terminator.
//         */
//        protected char[] eq;

//        /*
//         * <P>
//         * The character stored in this node: splitchar. Two special values are
//         * reserved:
//         * </P>
//         * <ul>
//         * <li>0x0000 as string terminator</li>
//         * <li>0xFFFF to indicate that the branch starting at this node is compressed</li>
//         * </ul>
//         * <p>
//         * This shouldn't be a problem if we give the usual semantics to strings since
//         * 0xFFFF is guaranteed not to be an Unicode character.
//         * </p>
//         */
//        protected char[] sc;

//        /*
//         * This vector holds the trailing of the keys when the branch is compressed.
//         */
//        protected CharVector kv;

//        protected char root;

//        protected char freenode;

//        protected int length; // number of items in tree

//        protected static readonly int BLOCK_SIZE = 2048; // allocation size for arrays

//        private TernaryTree()
//        {
//            Init();
//        }

//        protected void Init()
//        {
//            root = (char)0;
//            freenode = (char)1;
//            length = 0;
//            lo = new char[BLOCK_SIZE];
//            hi = new char[BLOCK_SIZE];
//            eq = new char[BLOCK_SIZE];
//            sc = new char[BLOCK_SIZE];
//            kv = new CharVector();
//        }

//        /*
//         * Branches are initially compressed, needing one node per key plus the size
//         * of the string key. They are decompressed as needed when another key with
//         * same prefix is inserted. This saves a lot of space, specially for long
//         * keys.
//         */
//        public void insert(String key, char val)
//        {
//            // make sure we have enough room in the arrays
//            int len = key.Length + 1; // maximum number of nodes that may be generated
//            if (freenode + len > eq.Length)
//            {
//                redimNodeArrays(eq.Length + BLOCK_SIZE);
//            }
//            char[] strkey = new char[len--];
//            key.GetChars(0, len, strkey, 0);
//            strkey[len] = (char)0;
//            root = insert(root, strkey, 0, val);
//        }

//        public void insert(char[] key, int start, char val)
//        {
//            int len = strlen(key) + 1;
//            if (freenode + len > eq.Length)
//            {
//                redimNodeArrays(eq.Length + BLOCK_SIZE);
//            }
//            root = insert(root, key, start, val);
//        }

//        /*
//         * The actual insertion function, recursive version.
//         */
//        private char insert(char p, char[] key, int start, char val)
//        {
//            int len = strlen(key, start);
//            if (p == 0)
//            {
//                // this means there is no branch, this node will start a new branch.
//                // Instead of doing that, we store the key somewhere else and create
//                // only one node with a pointer to the key
//                p = freenode++;
//                eq[p] = val; // holds data
//                length++;
//                hi[p] = (char)0;
//                if (len > 0)
//                {
//                    sc[p] = (char)0xFFFF; // indicates branch is compressed
//                    lo[p] = (char)kv.Alloc(len + 1); // use 'lo' to hold pointer to key
//                    strcpy(kv.GetArray(), lo[p], key, start);
//                }
//                else
//                {
//                    sc[p] = (char)0;
//                    lo[p] = (char)0;
//                }
//                return p;
//            }

//            if (sc[p] == 0xFFFF)
//            {
//                // branch is compressed: need to decompress
//                // this will generate garbage in the external key array
//                // but we can do some garbage collection later
//                char pp = freenode++;
//                lo[pp] = lo[p]; // previous pointer to key
//                eq[pp] = eq[p]; // previous pointer to data
//                lo[p] = (char)0;
//                if (len > 0)
//                {
//                    sc[p] = kv.Get(lo[pp]);
//                    eq[p] = pp;
//                    lo[pp]++;
//                    if (kv.Get(lo[pp]) == 0)
//                    {
//                        // key completly decompressed leaving garbage in key array
//                        lo[pp] = (char)0;
//                        sc[pp] = (char)0;
//                        hi[pp] = (char)0;
//                    }
//                    else
//                    {
//                        // we only got first char of key, rest is still there
//                        sc[pp] = (char)0xFFFF;
//                    }
//                }
//                else
//                {
//                    // In this case we can save a node by swapping the new node
//                    // with the compressed node
//                    sc[pp] = (char)0xFFFF;
//                    hi[p] = pp;
//                    sc[p] = (char)0;
//                    eq[p] = val;
//                    length++;
//                    return p;
//                }
//            }
//            char s = key[start];
//            if (s < sc[p])
//            {
//                lo[p] = insert(lo[p], key, start, val);
//            }
//            else if (s == sc[p])
//            {
//                if (s != 0)
//                {
//                    eq[p] = insert(eq[p], key, start + 1, val);
//                }
//                else
//                {
//                    // key already in tree, overwrite data
//                    eq[p] = val;
//                }
//            }
//            else
//            {
//                hi[p] = insert(hi[p], key, start, val);
//            }
//            return p;
//        }

//        /*
//         * Compares 2 null terminated char arrays
//         */
//        public static int strcmp(char[] a, int startA, char[] b, int startB)
//        {
//            for (; a[startA] == b[startB]; startA++, startB++)
//            {
//                if (a[startA] == 0)
//                {
//                    return 0;
//                }
//            }
//            return a[startA] - b[startB];
//        }

//        /*
//         * Compares a string with null terminated char array
//         */
//        public static int strcmp(String str, char[] a, int start)
//        {
//            int i, d, len = str.Length;
//            for (i = 0; i < len; i++)
//            {
//                d = (int)str[i] - a[start + i];
//                if (d != 0)
//                {
//                    return d;
//                }
//                if (a[start + i] == 0)
//                {
//                    return d;
//                }
//            }
//            if (a[start + i] != 0)
//            {
//                return (int)-a[start + i];
//            }
//            return 0;

//        }

//        public static void strcpy(char[] dst, int di, char[] src, int si)
//        {
//            while (src[si] != 0)
//            {
//                dst[di++] = src[si++];
//            }
//            dst[di] = (char)0;
//        }

//        public static int strlen(char[] a, int start)
//        {
//            int len = 0;
//            for (int i = start; i < a.Length && a[i] != 0; i++)
//            {
//                len++;
//            }
//            return len;
//        }

//        public static int strlen(char[] a)
//        {
//            return strlen(a, 0);
//        }

//        public int find(String key)
//        {
//            int len = key.Length;
//            char[] strkey = new char[len + 1];
//            key.GetChars(0, len, strkey, 0);
//            strkey[len] = (char)0;

//            return find(strkey, 0);
//        }

//        public int find(char[] key, int start)
//        {
//            int d;
//            char p = root;
//            int i = start;
//            char c;

//            while (p != 0)
//            {
//                if (sc[p] == 0xFFFF)
//                {
//                    if (strcmp(key, i, kv.GetArray(), lo[p]) == 0)
//                    {
//                        return eq[p];
//                    }
//                    else
//                    {
//                        return -1;
//                    }
//                }
//                c = key[i];
//                d = c - sc[p];
//                if (d == 0)
//                {
//                    if (c == 0)
//                    {
//                        return eq[p];
//                    }
//                    i++;
//                    p = eq[p];
//                }
//                else if (d < 0)
//                {
//                    p = lo[p];
//                }
//                else
//                {
//                    p = hi[p];
//                }
//            }
//            return -1;
//        }

//        public bool knows(String key)
//        {
//            return (find(key) >= 0);
//        }

//        // redimension the arrays
//        private void redimNodeArrays(int newsize)
//        {
//            int len = newsize < lo.Length ? newsize : lo.Length;
//            char[] na = new char[newsize];
//            Array.Copy(lo, 0, na, 0, len);
//            lo = na;
//            na = new char[newsize];
//            Array.Copy(hi, 0, na, 0, len);
//            hi = na;
//            na = new char[newsize];
//            Array.Copy(eq, 0, na, 0, len);
//            eq = na;
//            na = new char[newsize];
//            Array.Copy(sc, 0, na, 0, len);
//            sc = na;
//        }

//        public int size()
//        {
//            return length;
//        }

//        public Object clone()
//        {
//            TernaryTree t = new TernaryTree();
//            t.lo = (char[])this.lo.Clone();
//            t.hi = (char[])this.hi.Clone();
//            t.eq = (char[])this.eq.Clone();
//            t.sc = (char[])this.sc.Clone();
//            t.kv = (CharVector)this.kv.Clone();
//            t.root = this.root;
//            t.freenode = this.freenode;
//            t.length = this.length;

//            return t;
//        }

//        /*
//         * Recursively insert the median first and then the median of the lower and
//         * upper halves, and so on in order to get a balanced tree. The array of keys
//         * is assumed to be sorted in ascending order.
//         */
//        protected void insertBalanced(String[] k, char[] v, int offset, int n)
//        {
//            int m;
//            if (n < 1)
//            {
//                return;
//            }
//            m = n >> 1;

//            insert(k[m + offset], v[m + offset]);
//            insertBalanced(k, v, offset, m);

//            insertBalanced(k, v, offset + m + 1, n - m - 1);
//        }

//        /*
//         * Balance the tree for best search performance
//         */
//        public void balance()
//        {
//            // System.out.print("Before root splitchar = ");
//            // System.out.println(sc[root]);

//            int i = 0, n = length;
//            String[] k = new String[n];
//            char[] v = new char[n];
//            Iterator iter = new Iterator();
//            while (iter.HasMoreElements())
//            {
//                v[i] = iter.getValue();
//                k[i++] = (String)iter.nextElement();
//            }
//            Init();
//            insertBalanced(k, v, 0, n);

//            // With uniform letter distribution sc[root] should be around 'm'
//            // System.out.print("After root splitchar = ");
//            // System.out.println(sc[root]);
//        }

//        /*
//         * Each node stores a character (splitchar) which is part of some key(s). In a
//         * compressed branch (one that only contain a single string key) the trailer
//         * of the key which is not already in nodes is stored externally in the kv
//         * array. As items are inserted, key substrings decrease. Some substrings may
//         * completely disappear when the whole branch is totally decompressed. The
//         * tree is traversed to find the key substrings actually used. In addition,
//         * duplicate substrings are removed using a map (implemented with a
//         * TernaryTree!).
//         * 
//         */
//        public void trimToSize()
//        {
//            // first balance the tree for best performance
//            balance();

//            // redimension the node arrays
//            redimNodeArrays(freenode);

//            // ok, compact kv array
//            CharVector kx = new CharVector();
//            kx.Alloc(1);
//            TernaryTree map = new TernaryTree();
//            compact(kx, map, root);
//            kv = kx;
//            kv.TrimToSize();
//        }

//        private void compact(CharVector kx, TernaryTree map, char p)
//        {
//            int k;
//            if (p == 0)
//            {
//                return;
//            }
//            if (sc[p] == 0xFFFF)
//            {
//                k = map.find(kv.GetArray(), lo[p]);
//                if (k < 0)
//                {
//                    k = kx.Alloc(strlen(kv.GetArray(), lo[p]) + 1);
//                    strcpy(kx.GetArray(), k, kv.GetArray(), lo[p]);
//                    map.insert(kx.GetArray(), k, (char)k);
//                }
//                lo[p] = (char)k;
//            }
//            else
//            {
//                compact(kx, map, lo[p]);
//                if (sc[p] != 0)
//                {
//                    compact(kx, map, eq[p]);
//                }
//                compact(kx, map, hi[p]);
//            }
//        }

//        public IEnumerator keys()
//        {
//            return new Iterator();
//        }

//        public class Iterator : IEnumerator
//        {

//            /*
//             * current node index
//             */
//            int cur;

//            /*
//             * current key
//             */
//            String curkey;

//            private class Item : ICloneable
//            {
//                char parent;

//                char child;

//                public Item()
//                {
//                    parent = (char)0;
//                    child = (char)0;
//                }

//                public Item(char p, char c)
//                {
//                    parent = p;
//                    child = c;
//                }

//                public Object Clone()
//                {
//                    return new Item(parent, child);
//                }

//            }

//            /*
//             * Node stack
//             */
//            Stack ns;

//            /*
//             * key stack implemented with a StringBuilder
//             */
//            StringBuilder ks;

//            public Iterator()
//            {
//                cur = -1;
//                ns = new Stack();
//                ks = new StringBuilder();
//                rewind();
//            }

//            public void rewind()
//            {
//                ns.Clear();
//                ks.SetLength(0);
//                cur = root;
//                Run();
//            }

//            public Object nextElement()
//            {
//                String res = curkey;
//                cur = up();
//                Run();
//                return res;
//            }

//            public char getValue()
//            {
//                if (cur >= 0)
//                {
//                    return eq[cur];
//                }
//                return 0;
//            }

//            public bool hasMoreElements()
//            {
//                return (cur != -1);
//            }

//            /*
//             * traverse upwards
//             */
//            private int up()
//            {
//                Item i = new Item();
//                int res = 0;

//                if (ns.Empty())
//                {
//                    return -1;
//                }

//                if (cur != 0 && sc[cur] == 0)
//                {
//                    return lo[cur];
//                }

//                bool climb = true;

//                while (climb)
//                {
//                    i = (Item)ns.Pop();
//                    i.child++;
//                    switch (i.child)
//                    {
//                        case 1:
//                            if (sc[i.parent] != 0)
//                            {
//                                res = eq[i.parent];
//                                ns.Push(i.Clone());
//                                ks.Append(sc[i.parent]);
//                            }
//                            else
//                            {
//                                i.child++;
//                                ns.Push(i.Clone());
//                                res = hi[i.parent];
//                            }
//                            climb = false;
//                            break;

//                        case 2:
//                            res = hi[i.parent];
//                            ns.Push(i.Clone());
//                            if (ks.Length() > 0)
//                            {
//                                ks.SetLength(ks.Length() - 1); // pop
//                            }
//                            climb = false;
//                            break;

//                        default:
//                            if (ns.Clear())
//                            {
//                                return -1;
//                            }
//                            climb = true;
//                            break;
//                    }
//                }
//                return res;
//            }

//            /*
//             * traverse the tree to find next key
//             */
//            private int Run()
//            {
//                if (cur == -1)
//                {
//                    return -1;
//                }

//                bool leaf = false;
//                while (true)
//                {
//                    // first go down on low branch until leaf or compressed branch
//                    while (cur != 0)
//                    {
//                        if (sc[cur] == 0xFFFF)
//                        {
//                            leaf = true;
//                            break;
//                        }
//                        ns.Push(new Item((char)cur, '\u0000'));
//                        if (sc[cur] == 0)
//                        {
//                            leaf = true;
//                            break;
//                        }
//                        cur = lo[cur];
//                    }
//                    if (leaf)
//                    {
//                        break;
//                    }
//                    // nothing found, go up one node and try again
//                    cur = up();
//                    if (cur == -1)
//                    {
//                        return -1;
//                    }
//                }
//                // The current node should be a data node and
//                // the key should be in the key stack (at least partially)
//                StringBuilder buf = new StringBuilder(ks.ToString());
//                if (sc[cur] == 0xFFFF)
//                {
//                    int p = lo[cur];
//                    while (kv.Get(p) != 0)
//                    {
//                        buf.Append(kv.Get(p++));
//                    }
//                }
//                curkey = buf.ToString();
//                return 0;
//            }

//        }

//        public void PrintStats()
//        {
//            Console.WriteLine("Number of keys = " + length);
//            Console.WriteLine("Node count = " + freenode);
//            // System.out.println("Array length = " + Integer.toString(eq.length));
//            Console.WriteLine("Key Array length = " + kv.Length());

//            /*
//             * for(int i=0; i<kv.length(); i++) if ( kv.get(i) != 0 )
//             * System.out.print(kv.get(i)); else System.out.println("");
//             * System.out.println("Keys:"); for(Enumeration enum = keys();
//             * enum.hasMoreElements(); ) System.out.println(enum.nextElement());
//             */

//        }

//        public static void Main(String[] args)
//        {
//            TernaryTree tt = new TernaryTree();
//            tt.insert("Carlos", 'C');
//            tt.insert("Car", 'r');
//            tt.insert("palos", 'l');
//            tt.insert("pa", 'p');
//            tt.trimToSize();
//            Console.WriteLine((char)tt.find("Car"));
//            Console.WriteLine((char)tt.find("Carlos"));
//            Console.WriteLine((char)tt.find("alto"));
//            tt.PrintStats();
//        }
//    }
//}