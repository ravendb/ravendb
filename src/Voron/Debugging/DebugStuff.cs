using System;
using System.Diagnostics;
using System.IO;
using Sparrow.Platform;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Impl;

namespace Voron.Debugging
{
    public class DebugStuff
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
        public static void RenderAndShow_FixedSizeTree(LowLevelTransaction tx, FixedSizeTree fst)
        {
            var name = fst.Name;
            var tree = fst.Parent;
            RenderHtmlTreeView(writer =>
            {
                DumpFixedSizeTreeToStream(tx, fst, writer, name, tree);
            });
        }

        private static unsafe void DumpFixedSizeTreeToStream(LowLevelTransaction tx, FixedSizeTree fst, TextWriter writer, Slice name, Tree tree)
        {
            var ptr = tree.DirectRead(name);
            if (ptr == null)
            {
                writer.WriteLine("<p>empty fixed size tree</p>");
            }
            else if (((FixedSizeTreeHeader.Embedded*)ptr)->RootObjectType == RootObjectType.EmbeddedFixedSizeTree)
            {
                var header = ((FixedSizeTreeHeader.Embedded*)ptr);
                writer.WriteLine("<p>Number of entries: {0:#,#;;0}, val size: {1:#,#;;0}.</p>", header->NumberOfEntries, header->ValueSize);
                writer.WriteLine("<ul>");
                var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
                for (int i = 0; i < header->NumberOfEntries; i++)
                {
                    var key = *(long*)(dataStart + ((sizeof(long) + header->ValueSize) * i));
                    writer.WriteLine("<li>{0:#,#;;0}</li>", key);
                }
                writer.WriteLine("</ul>");
            }
            else
            {
                var header = (FixedSizeTreeHeader.Large*)ptr;
                writer.WriteLine("<p>Number of entries: {0:#,#;;0}, val size: {1:#,#;;0}.</p>", header->NumberOfEntries, header->ValueSize);
                writer.WriteLine("<div class='css-treeview'><ul>");

                var page = fst.GetReadOnlyPage(header->RootPageNumber);

                RenderFixedSizeTreePage(tx, page, writer, header, "Root", true);

                writer.WriteLine("</ul></div>");
            }
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
                var process = new Process
                {
                    StartInfo =
                    {
                        FileName = @"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
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

        private unsafe static void RenderFixedSizeTreePage(LowLevelTransaction tx, FixedSizeTreePage page, TextWriter sw, FixedSizeTreeHeader.Large* header, string text, bool open)
        {
            sw.WriteLine(
                "<ul><li><input type='checkbox' id='page-{0}' {3} /><label for='page-{0}'>{4}: Page {0:#,#;;0} - {1} - {2:#,#;;0} entries</label><ul>",
                page.PageNumber, page.IsLeaf ? "Leaf" : "Branch", page.NumberOfEntries, open ? "checked" : "", text);

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                if (page.IsLeaf)
                {
                    var key =
                        *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + header->ValueSize)) * i));
                    sw.Write("{0:#,#;;0}, ", key);
                }
                else
                {
                    var key =
                     *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + sizeof(long))) * i));
                    var pageNum = *(long*)(page.Pointer + page.StartPosition + (((sizeof(long) + sizeof(long))) * i) + sizeof(long));

                    var s = key.ToString("#,#");
                    if (i == 0)
                    {
                        if (key == long.MinValue)
                            s = "[smallest]";
                        else
                            s = "[smallest " + s + "]";
                    }

                    var fstPage = tx.GetPage(pageNum);
                    RenderFixedSizeTreePage(tx, new FixedSizeTreePage(fstPage.Pointer, header->ValueSize + sizeof(long), Constants.Storage.PageSize), sw, header, s, false);
                }
            }

            sw.WriteLine("</ul></li></ul>");
        }

        [Conditional("DEBUG")]
        public static void RenderAndShow(Tree tree)
        {
            var headerData = string.Format("<p>{0}</p>", tree.State);
            RenderAndShowTree(tree, tree.State.RootPageNumber, headerData);
        }

        [Conditional("DEBUG")]
        public static void RenderAndShowTree(Tree tree, long startPageNumber, string headerData = null)
        {
            RenderHtmlTreeView(writer =>
            {
                if (headerData != null)
                    writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");

                RenderPage(tree, tree.GetReadOnlyTreePage(startPageNumber), writer, "Root", true);

                writer.WriteLine("</ul></div>");
            });

        }

        public static void DumpTreeToStream(Tree tree, Stream stream)
        {
            var headerData = string.Format("<p>{0}</p>", tree.State);

            WriteHtml(new StreamWriter(stream), writer =>
            {
                writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");

                RenderPage(tree, tree.GetReadOnlyTreePage(tree.State.RootPageNumber), writer, "Root", true);

                writer.WriteLine("</ul></div>");
            });
        }

        public static void DumpFixedSizedTreeToStream(LowLevelTransaction tx, FixedSizeTree tree, Stream stream)
        {
            var headerData = $"{tree.Name} ({tree.Type}) {tree.NumberOfEntries} entries, depth: {tree.Depth}, {tree.PageCount} pages.";

            WriteHtml(new StreamWriter(stream), writer =>
            {
                writer.WriteLine(headerData);
                writer.WriteLine("<div class='css-treeview'><ul>");

                DumpFixedSizeTreeToStream(tx, tree, writer, tree.Name, tree.Parent);

                writer.WriteLine("</ul></div>");
            });
        }


        private static void WriteHtml(TextWriter sw, Action<TextWriter> action)
        {
            sw.WriteLine("<html><head><style>{0}</style></head><body>", css);
            action(sw);
            sw.WriteLine("</body></html>");
            sw.Flush();
        }

        private static unsafe void RenderPage(Tree tree, TreePage page, TextWriter sw, string text, bool open)
        {
            sw.WriteLine(
               "<ul><li><input type='checkbox' id='page-{0}' {3} /><label for='page-{0}'>{4}: Page {0:#,#;;0} - {1} - {2:#,#;;0} entries {5}</label><ul>",
               page.PageNumber, page.IsLeaf ? "Leaf" : "Branch", page.NumberOfEntries, open ? "checked" : "", text, 
               page.IsCompressed? $"(Compressed ({page.CompressionHeader->NumberOfCompressedEntries} entries [uncompressed/compressed: {page.CompressionHeader->UncompressedSize}/{page.CompressionHeader->CompressedSize}])" : string.Empty);

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var nodeHeader = page.GetNode(i);
                
                string key;
                Slice keySlice;
                using (TreeNodeHeader.ToSlicePtr(tree.Llt.Allocator, nodeHeader, out keySlice))
                {
                    key = keySlice.ToString();
                }

                if (page.IsLeaf)
                {               
                    sw.Write("<li>{0} {1} - size: {2:#,#}</li>", key, nodeHeader->Flags, tree.GetDataSize(nodeHeader));
                }
                else
                {
                    var pageNum = nodeHeader->PageNumber;

                    if (i == 0)
                        key = "[smallest]";

                    RenderPage(tree, tree.GetReadOnlyTreePage(pageNum), sw, key, false);
                }
            }
            sw.WriteLine("</ul></li></ul>");
        }
    }
}
