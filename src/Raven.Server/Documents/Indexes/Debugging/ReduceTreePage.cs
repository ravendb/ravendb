using System;
using System.Collections.Generic;
using Sparrow.Json;
using Voron.Data.BTrees;
using Voron.Data.Compression;

namespace Raven.Server.Documents.Indexes.Debugging
{
    public class ReduceTreePage : IDisposable
    {
        private bool _disposed;
        private readonly bool _isLeaf;

        public ReduceTreePage()
        {
        }

        ~ReduceTreePage()
        {
            Dispose();
        }

        public ReduceTreePage(TreePage p)
        {
            Page = p;

            if (Page.IsLeaf)
            {
                _isLeaf = true;
                Entries = new List<MapResultInLeaf>(Page.NumberOfEntries);
            }
            else
                Children = new List<ReduceTreePage>(Page.NumberOfEntries);
        }

        public TreePage Page { get; }

        public long PageNumber => Page.PageNumber;

        public readonly List<ReduceTreePage> Children;

        public readonly List<MapResultInLeaf> Entries;

        public BlittableJsonReaderObject AggregationResult;

        public DecompressedLeafPage DecompressedLeaf;

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_isLeaf)
                DecompressedLeaf?.Dispose();
            else
            {
                foreach (var reduceTreePage in Children)
                {
                    reduceTreePage.Dispose();
                }
            }

            GC.SuppressFinalize(this);
        }
    }
}