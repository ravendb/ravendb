using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Threading.Tasks;
using Sparrow.Platform;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;

namespace Voron.Debugging
{
    public sealed class DebugStuff
    {

        private const string css = @".css-treeview ul,
.css-treeview li
{
    padding: 0;
    margin: 0;
    list-style: none;
}
 
.css-treeview input
{
    position: absolute;
    opacity: 0;
}
 
.css-treeview
{
    font: normal 11px 'Segoe UI', Arial, Sans-serif;
    -moz-user-select: none;
    -webkit-user-select: none;
    user-select: none;
}
 
.css-treeview a
{
    color: #00f;
    text-decoration: none;
}
 
.css-treeview a:hover
{
    text-decoration: underline;
}
 
.css-treeview input + label + ul
{
    margin: 0 0 0 22px;
}
 
.css-treeview input ~ ul
{
    display: none;
}
 
.css-treeview label,
.css-treeview label::before
{
    cursor: pointer;
}
 
.css-treeview input:disabled + label
{
    cursor: default;
    opacity: .6;
}
 
.css-treeview input:checked:not(:disabled) ~ ul
{
    display: block;
}
 
.css-treeview label,
.css-treeview label::before
{
    background: url('http://experiments.wemakesites.net/pages/css3-treeview/example/icons.png') no-repeat;
}
 
.css-treeview label,
.css-treeview a,
.css-treeview label::before
{
    display: inline-block;
    height: 16px;
    line-height: 16px;
    vertical-align: middle;
}
 
.css-treeview label
{
    background-position: 18px 0;
}
 
.css-treeview label::before
{
    content: '';
    width: 16px;
    margin: 0 22px 0 0;
    vertical-align: middle;
    background-position: 0 -32px;
}
 
.css-treeview input:checked + label::before
{
    background-position: 0 -16px;
}
 
/* webkit adjacent element selector bugfix */
@media screen and (-webkit-min-device-pixel-ratio:0)
{
    .css-treeview 
    {
        -webkit-animation: webkit-adjacent-element-selector-bugfix infinite 1s;
    }
 
    @-webkit-keyframes webkit-adjacent-element-selector-bugfix 
    {
        from 
        { 
            padding: 0;
        } 
        to 
        { 
            padding: 0;
        }
    }
}";

        [Conditional("DEBUG")]
        public static void RenderAndShow_FixedSizeTree<TVal>(LowLevelTransaction tx, FixedSizeTree<TVal> fst) 
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            var name = fst.Name;
            var tree = fst.Parent;
            RenderHtmlTreeView(writer =>
            {
                DumpFixedSizeTreeToStreamAsync(tx, fst, writer, name, tree).Wait();
            });
        }

        private static unsafe Task DumpFixedSizeTreeToStreamAsync<TVal>(LowLevelTransaction tx, FixedSizeTree<TVal> fst, TextWriter writer, Slice name, Tree tree)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            var ptr = tree.DirectRead(name);
            if (ptr == null)
                return RenderEmptyFixedSizeTreeAsync(writer);

            if (((FixedSizeTreeHeader.Embedded*)ptr)->RootObjectType == RootObjectType.EmbeddedFixedSizeTree)
            {
                var embedded = new FixedSizeTreeSafe.EmbeddedFixedSizeTreeSafe(ptr);

                return RenderEmbeddedFixedSizeTreeAsync(embedded, writer);
            }

            var large = new FixedSizeTreeSafe.LargeFixedSizeTreeSafe(ptr);
            return RenderLargeFixedSizeTreeAsync(large, tx, fst, writer);
        }

        private static async Task RenderLargeFixedSizeTreeAsync<TVal>(FixedSizeTreeSafe.LargeFixedSizeTreeSafe tree, LowLevelTransaction tx, FixedSizeTree<TVal> fst, TextWriter writer)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            await writer.WriteLineAsync(string.Format("<p>Number of entries: {0:#,#;;0}, val size: {1:#,#;;0}.</p>", tree.NumberOfEntries, tree.ValueSize));
            await writer.WriteLineAsync("<div class='css-treeview'><ul>");

            var page = fst.GetReadOnlyPage(tree.RootPageNumber);

            await RenderFixedSizeTreePageAsync(tx, page, writer, tree, "Root", true);

            await writer.WriteLineAsync("</ul></div>");
        }

        private static async Task RenderEmbeddedFixedSizeTreeAsync(FixedSizeTreeSafe.EmbeddedFixedSizeTreeSafe tree, TextWriter writer)
        {
            await writer.WriteLineAsync(string.Format("<p>Number of entries: {0:#,#;;0}, val size: {1:#,#;;0}.</p>", tree.NumberOfEntries, tree.ValueSize));
            await writer.WriteLineAsync("<ul>");

            foreach (var key in tree.GetKeys())
            {
                await writer.WriteLineAsync(string.Format("<li>{0:#,#;;0}</li>", key));
            }

            await writer.WriteLineAsync("</ul>");
        }

        private static Task RenderEmptyFixedSizeTreeAsync(TextWriter writer)
        {
            return writer.WriteLineAsync("<p>empty fixed size tree</p>");
        }

        private static void RenderHtmlTreeView(Action<TextWriter> action)
        {
            if (Debugger.IsAttached == false)
                return;

            var output = Path.GetTempFileName() + ".html";
            using (var file = File.OpenWrite(output))
            using (var sw = new StreamWriter(file))
            {
                sw.WriteLine("<html><head><style>{0}</style></head><body>", css);
                action(sw);
                sw.WriteLine("</body></html>");
                sw.Flush();
            }

            if (PlatformDetails.RunningOnPosix == false)
            {
                string pathToChromeX86 = @"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe";
                string pathToChromeX64 = @"C:\Program Files\Google\Chrome\Application\chrome.exe";

                string pathToChrome;

                if (File.Exists(pathToChromeX64))
                    pathToChrome = pathToChromeX64;
                else if (File.Exists(pathToChromeX86))
                    pathToChrome = pathToChromeX86;
                else
                    throw new InvalidOperationException("Make sure path to chrome.exe is valid");

                var process = new Process
                {
                    StartInfo =
                    {
                        FileName = pathToChrome,
                        Arguments = output,
                        UseShellExecute = false,
                        CreateNoWindow = true,
                        RedirectStandardError = true,
                    }
                };
                process.Start();
                return;
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                Process.Start("open", output);
            }
            else
            {
                Process.Start("xdg-open", output);
            }

        }

        private static async Task RenderFixedSizeTreePageAsync<TVal>(LowLevelTransaction tx, FixedSizeTreePage<TVal> page, TextWriter sw, FixedSizeTreeSafe.LargeFixedSizeTreeSafe tree, string text, bool open)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            await sw.WriteLineAsync(
                string.Format("<ul><li><input type='checkbox' id='page-{0}' {3} /><label for='page-{0}'>{4}: Page {0:#,#;;0} - {1} - {2:#,#;;0} entries</label><ul>",
                page.PageNumber, page.IsLeaf ? "Leaf" : "Branch", page.NumberOfEntries, open ? "checked" : "", text));

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var key = GetFixedSizeTreeKey(page, tree, i);

                if (page.IsLeaf)
                {
                    await sw.WriteAsync(string.Format("{0:#,#;;0}, ", key));
                }
                else
                {
                    var pageNum = GetFixedSizeTreePageNumber(page, i);

                    var s = key.ToString("#,#");
                    if (i == 0)
                    {
                        if (key == long.MinValue)
                            s = "[smallest]";
                        else
                            s = "[smallest " + s + "]";
                    }

                    var fstPage = tx.GetPage(pageNum);
                    await RenderFixedSizeTreePageAsync(tx, CreateFixedSizeTreePage<TVal>(fstPage, tree), sw, tree, s, false);
                }
            }

            await sw.WriteLineAsync("</ul></li></ul>");
        }
        
        [Conditional("DEBUG")]
        public static void RenderAndShow<TKey>(Lookup<TKey> tree, int steps = 1, string message = null)
            where TKey : struct, ILookupKey
        {
            var headerData = $"{tree.State}";
            if (!string.IsNullOrWhiteSpace(message))
                headerData = $"{message}-{headerData}";

            RenderAndShowTCompactTree(tree, steps, tree.State.RootPage, $"<p>{headerData}</p>");
        }

        [Conditional("DEBUG")]
        public static void RenderAndShow(CompactTree tree, string message = null)
        {
            RenderAndShow(tree._inner);
        }

        [Conditional("DEBUG")]
        public static void RenderAndShowTCompactTree<TKey>(Lookup<TKey> tree, int steps, long startPageNumber, string headerData = null)
            where TKey : struct, ILookupKey
        {
            RenderHtmlTreeView(writer =>
            {
                if (headerData != null)
                    writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");
                               RenderPageInternal(tree, steps, tree.Llt.GetPage(startPageNumber), writer, "Root", true);

                writer.WriteLine("</ul></div>");
            });
        }
        
        private static unsafe void RenderPageInternal<TKey>(Lookup<TKey> tree, int steps, Page page, TextWriter sw, string text, bool open)
            where TKey : struct, ILookupKey
        {
            var header = (LookupPageHeader*)page.Pointer;
            sw.WriteLine($"<ul><li><input type='checkbox' id='page-{page.PageNumber}' {(open ? "checked" : "")} /><label for='page-{page.PageNumber}'>{text}: Page {page.PageNumber:#,#;;0} - {header->PageFlags} - {header->NumberOfEntries:#,#;;0} entries - Usable space: {header->Upper - header->Lower} / Available {header->FreeSpace}</label><ul>");

            var entries = new Span<ushort>(page.Pointer + PageHeader.SizeOf, header->NumberOfEntries);
            for (int i = 0; i < header->NumberOfEntries; i++)
            {
                var state = new Lookup<TKey>.CursorState { Page = page };
                Lookup<TKey>.GetKeyAndValue(ref state, i, out var key, out var val);
                TKey lookupKey = TKey.FromLong<TKey>(key);

                if (header->PageFlags.HasFlag(LookupPageFlags.Leaf))
                {
                    if (i % steps == 0)
                    {
                        sw.Write($"<li>{lookupKey.ToString(tree)} --> {val}</li>");
                    }
                }
                else
                {
                    RenderPageInternal(tree, steps, tree.Llt.GetPage(val), sw, lookupKey.ToString(tree), false);
                }
            }

            sw.WriteLine("</ul></li></ul>");
        }


        private static unsafe long GetFixedSizeTreeKey<TVal>(FixedSizeTreePage<TVal> page, FixedSizeTreeSafe.LargeFixedSizeTreeSafe tree, int i)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            if (page.IsLeaf)
                return *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + tree.ValueSize)) * i));

            return *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + sizeof(long))) * i));
        }

        private static unsafe long GetFixedSizeTreePageNumber<TVal>(FixedSizeTreePage<TVal> page, int i)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            return *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + sizeof(long))) * i) + sizeof(long));
        }

        private static unsafe FixedSizeTreePage<TVal> CreateFixedSizeTreePage<TVal>(Page page, FixedSizeTreeSafe.LargeFixedSizeTreeSafe tree)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            return new FixedSizeTreePage<TVal>(page.Pointer, tree.ValueSize + sizeof(long), Constants.Storage.PageSize);
        }

 

        [Conditional("DEBUG")]
        public static void RenderAndShow(PostingList tree)
        {
            var headerData = $"<p>{tree.State}</p>";
            RenderAndShowTCompactTree(tree, tree.State.RootPage, headerData);
        }

        [Conditional("DEBUG")]
        public static void RenderAndShowTCompactTree(PostingList tree, long startPageNumber, string headerData = null)
        {
            RenderHtmlTreeView(writer =>
            {
                if (headerData != null)
                    writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");

                RenderPageInternal(tree, tree.Llt.GetPage(startPageNumber), writer, "Root", true);

                writer.WriteLine("</ul></div>");
            });
        }

        private static unsafe void RenderPageInternal(PostingList tree, Page page, TextWriter sw, string text, bool open)
        {
            var header = new PostingListCursorState { Page = page };
            var leaf = new PostingListLeafPage(page);
            var branch = new PostingListBranchPage(page);

            sw.WriteLine(
                string.Format("<ul><li><input type='checkbox' id='page-{0}' {3} /><label for='page-{0}'>{4}: Page {0:#,#;;0} - {1} - {2:#,#;;0} entries - {5}</label><ul>",
                    page.PageNumber, header.IsLeaf ? "leaf" : "branch", header.IsLeaf ? leaf.Header->NumberOfEntries : branch.Header->NumberOfEntries, open ? "checked" : "", text, 
                    header.IsLeaf ? leaf.SpaceUsed + " used" : ""));

            if (header.IsLeaf)
            {
                //sw.WriteLine(
                //    string.Format("<ul><li><input type='checkbox' id='page-{0}-details'/><label for='page-{0}-details'>Compression details</label><ul>",
                //        page.PageNumber));
                var leafEntries = leaf.GetDebugOutput();
                sw.WriteLine($"<li>Entries {leaf.Header->NumberOfEntries:#,#;;0} in {leaf.Header->SizeUsed:#,#;;0} bytes</li>");
                if (leafEntries.Count > 0)
                {
                    sw.WriteLine($"<li>Range {leafEntries[0]:#,#;;0} ... {leafEntries[^1]:#,#;;0}</li>");
                }

                //sw.WriteLine("</ul></li></ul>");

                //foreach (long val in leafEntries!)
                //{
                //    sw.Write($"<li>{val:#,#;;0}</li>");
                //}
            }
            else
            {
                for (int i = 0; i < branch.Header->NumberOfEntries; i++)
                {
                    (long key, long pageNum) = branch.GetByIndex(i);
                    RenderPageInternal(tree, tree.Llt.GetPage(pageNum), sw, key.ToString("#,#;;0"), false);
                }
            }

            sw.WriteLine("</ul></li></ul>");
        }

        [Conditional("DEBUG")]
        public static void RenderAndShow(Tree tree, bool decompress = false)
        {
            TreeRootHeader header = tree.ReadHeader();
            var headerData = $"<p>{header}</p>";
            RenderAndShowTree(tree, header.RootPageNumber, headerData, decompress);
        }

        [Conditional("DEBUG")]
        public static void RenderAndShowTree(Tree tree, long startPageNumber, string headerData = null, bool decompress = false)
        {
            RenderHtmlTreeView(writer =>
            {
                if (headerData != null)
                    writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");

                RenderPageAsync(tree, tree.GetReadOnlyTreePage(startPageNumber), writer, "Root", true, decompress).Wait();

                writer.WriteLine("</ul></div>");
            });

        }

        public static Task DumpTreeToStreamAsync(Tree tree, Stream stream)
        {
            TreeRootHeader header = tree.ReadHeader();
            var headerData = $"<p>{header}</p>";

            return WriteHtmlAsync(new StreamWriter(stream), async writer =>
            {
                await writer.WriteLineAsync(headerData);
                await writer.WriteLineAsync("<div class='css-treeview'><ul>");

                await RenderPageAsync(tree, tree.GetReadOnlyTreePage(header.RootPageNumber), writer, "Root", true, false);

                await writer.WriteLineAsync("</ul></div>");
            });
        }

        public static Task DumpFixedSizedTreeToStreamAsync(LowLevelTransaction tx, FixedSizeTree tree, Stream stream)
        {
            var headerData = $"{tree.Name} ({tree.Type}) {tree.NumberOfEntries} entries, depth: {tree.Depth}, {tree.PageCount} pages.";

            return WriteHtmlAsync(new StreamWriter(stream), async writer =>
            {
                await writer.WriteLineAsync(headerData);
                await writer.WriteLineAsync("<div class='css-treeview'><ul>");

                await DumpFixedSizeTreeToStreamAsync(tx, tree, writer, tree.Name, tree.Parent);

                await writer.WriteLineAsync("</ul></div>");
            });
        }


        private static async Task WriteHtmlAsync(TextWriter sw, Func<TextWriter, Task> action)
        {
            await sw.WriteLineAsync($"<html><head><style>{css}</style></head><body>");
            await action(sw);
            await sw.WriteLineAsync("</body></html>");
            await sw.FlushAsync();
        }

        private static Task RenderPageAsync(Tree tree, TreePage page, TextWriter sw, string text, bool open, bool decompress)
        {
            var treePage = new TreePageSafe(tree, page);

            return RenderPageInternalAsync(tree, treePage, sw, text, open, decompress);
        }

        private static async Task RenderPageInternalAsync(Tree tree, TreePageSafe page, TextWriter sw, string text, bool open, bool decompress)
        {
            await sw.WriteLineAsync(
                string.Format("<ul><li><input type='checkbox' id='page-{0}' {3} /><label for='page-{0}'>{4}: Page {0:#,#;;0} - {1} - {2:#,#;;0} entries {5}</label><ul>",
                    page.PageNumber, page.IsLeaf ? "Leaf" : "Branch", page.NumberOfEntries, open ? "checked" : "", text,
                    page.IsCompressed ? $"(Compressed ({page.NumberOfCompressedEntries} entries [uncompressed/compressed: {page.UncompressedSize}/{page.CompressedSize}])" : string.Empty));

            DecompressedLeafPage decompressedPage = null;

            if (page.IsCompressed && decompress)
            {
                decompressedPage = tree.DecompressPage(page.TreePage, DecompressionUsage.Read, skipCache: true);

                page = new TreePageSafe(page.Tree, decompressedPage);
            }

            try
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var nodeHeader = page.GetNode(i);

                    string key = nodeHeader.Key;

                    if (page.IsLeaf)
                    {
                        await sw.WriteAsync(string.Format("<li>{0} {1} - size: {2:#,#}</li>", key, nodeHeader.Flags, nodeHeader.GetDataSize()));
                    }
                    else
                    {
                        var pageNum = nodeHeader.PageNumber;

                        if (i == 0)
                            key = "[smallest]";

                        await RenderPageAsync(tree, tree.GetReadOnlyTreePage(pageNum), sw, key, false, decompress);
                    }
                }
            }
            finally
            {
                decompressedPage?.Dispose();
            }

            await sw.WriteLineAsync("</ul></li></ul>");
        }

        private sealed unsafe class TreePageSafe
        {
            private readonly Tree _tree;
            private readonly TreePage _page;

            public TreePageSafe(Tree tree, TreePage page)
            {
                _tree = tree ?? throw new ArgumentNullException(nameof(tree));
                _page = page ?? throw new ArgumentNullException(nameof(page));
            }

            public Tree Tree => _tree;

            public TreePage TreePage => _page;

            public bool IsLeaf => _page.IsLeaf;

            public long PageNumber => _page.PageNumber;

            public ushort NumberOfEntries => _page.NumberOfEntries;

            public bool IsCompressed => _page.IsCompressed;

            public ushort? NumberOfCompressedEntries
            {
                get
                {
                    if (IsCompressed)
                        return _page.CompressionHeader->NumberOfCompressedEntries;

                    return null;
                }
            }

            public ushort? UncompressedSize
            {
                get
                {
                    if (IsCompressed)
                        return _page.CompressionHeader->UncompressedSize;

                    return null;
                }
            }

            public ushort? CompressedSize
            {
                get
                {
                    if (IsCompressed)
                        return _page.CompressionHeader->CompressedSize;

                    return null;
                }
            }

            public TreeNodeHeaderSafe GetNode(int n)
            {
                return new TreeNodeHeaderSafe(_tree, _page.GetNode(n));
            }

            public sealed class TreeNodeHeaderSafe
            {
                private readonly Tree _tree;
                private readonly TreeNodeHeader* _nodeHeader;

                public TreeNodeHeaderSafe(Tree tree, TreeNodeHeader* nodeHeader)
                {
                    _tree = tree ?? throw new ArgumentNullException(nameof(tree));
                    _nodeHeader = nodeHeader;

                    using (TreeNodeHeader.ToSlicePtr(tree.Llt.Allocator, nodeHeader, out Slice keySlice))
                    {
                        // uncomment to convert the slice to long
                        //if (keySlice.Size == sizeof(long))
                        //{
                        //    var idPtr = (long*)keySlice.Content.Ptr;
                        //    var id = Bits.SwapBytes(*idPtr);
                        //    Key = id.ToString();
                        //}
                        //else
                        Key = keySlice.ToString();
                    }
                }

                public string Key { get; }

                public long PageNumber => _nodeHeader->PageNumber;

                public TreeNodeFlags Flags => _nodeHeader->Flags;

                public int GetDataSize()
                {
                    return _tree.GetDataSize(_nodeHeader);
                }
            }
        }

        private sealed class FixedSizeTreeSafe
        {
            private FixedSizeTreeSafe()
            {
            }

            public sealed unsafe class EmbeddedFixedSizeTreeSafe
            {
                private readonly byte* _ptr;

                private readonly FixedSizeTreeHeader.Embedded* _header;

                public EmbeddedFixedSizeTreeSafe(byte* ptr)
                {
                    _ptr = ptr;
                    _header = ((FixedSizeTreeHeader.Embedded*)ptr);
                }

                public ushort NumberOfEntries => _header->NumberOfEntries;

                public ushort ValueSize => _header->ValueSize;

                public List<long> GetKeys()
                {
                    var dataStart = _ptr + sizeof(FixedSizeTreeHeader.Embedded);

                    var keys = new List<long>();
                    for (int i = 0; i < NumberOfEntries; i++)
                    {
                        var key = *(long*)(dataStart + ((sizeof(long) + ValueSize) * i));
                        keys.Add(key);
                    }

                    return keys;
                }
            }

            public sealed unsafe class LargeFixedSizeTreeSafe
            {
                private readonly FixedSizeTreeHeader.Large* _header;

                public LargeFixedSizeTreeSafe(byte* ptr)
                {
                    _header = (FixedSizeTreeHeader.Large*)ptr;
                }

                public long NumberOfEntries => _header->NumberOfEntries;

                public ushort ValueSize => _header->ValueSize;

                public long RootPageNumber => _header->RootPageNumber;
            }
        }
    }
}
