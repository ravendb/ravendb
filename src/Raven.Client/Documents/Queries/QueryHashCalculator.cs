using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    public unsafe struct QueryHashCalculator : IDisposable
    {
        private UnmanagedWriteBuffer _buffer;

        public QueryHashCalculator(JsonOperationContext ctx)
        {
            _buffer = ctx.GetStream();
        }

        public ulong GetHash()
        {
            _buffer.EnsureSingleChunk(out var ptr, out var size);

            return Hashing.XXHash64.Calculate(ptr, (ulong)size);
        }

        public void Write(float f)
        {
            _buffer.Write((byte*)&f, sizeof(float));
        }
        
        public void Write(long l)
        {
            _buffer.Write((byte*)&l, sizeof(long));
        }

        public void Write(long? l)
        {
            if (l == null)
                return;
            Write(l.Value);
        }
        
        public void Write(float? f)
        {
            if (f == null)
                return;
            Write(f.Value);
        }
        
        public void Write(int? i)
        {
            if (i == null)
                return;
            Write(i.Value);
        }


        public void Write(int i)
        {
            _buffer.Write((byte*)&i, sizeof(int));
        }

        public void Write(bool b)
        {
            if (b)
                _buffer.WriteByte(1);
        }
        
        public void Write(bool? b)
        {
            if (b != null)
                _buffer.WriteByte(b.Value ? (byte)1 : (byte)2);
        }

        public void Write(string s)
        {
            if (s == null) return;
            fixed (char* pQ = s)
                _buffer.Write((byte*)pQ, length: s.Length * sizeof(char));
        }

        public void Write(string[] s)
        {
            if (s == null) return;
            for (int i = 0; i < s.Length; i++)
            {
                Write(s[i]);
            }
        }
        
        public void Write(List<string> s)
        {
            if (s == null) return;
            for (int i = 0; i < s.Count; i++)
            {
                Write(s[i]);
            }
        }

        public void Dispose()
        {
            _buffer.Dispose();
        }

        public void Write(HighlightedField[] highlightedFields)
        {
            if (highlightedFields == null) return;
            for (int i = 0; i < highlightedFields.Length; i++)
            {
                Write(highlightedFields[i].Field);
                Write(highlightedFields[i].FragmentCount);
                Write(highlightedFields[i].FragmentLength);
                Write(highlightedFields[i].FragmentsField);
            }
        }

        public void Write(Dictionary<string, object> qp)
        {
            if (qp == null)
                return;
            foreach (var kvp in qp)
            {
                Write(kvp.Key);
                if (kvp.Value is string s)
                {
                    Write(s);
                }
                else if (kvp.Value is long l)
                {
                    Write(l);
                }
                else if (kvp.Value is int i)
                {
                    Write(i);
                }
                else if (kvp.Value is bool b)
                {
                    Write(b);
                }
                else if (kvp.Value == null)
                {
                    // write nothing
                }
                else
                {
                    Write(kvp.Value.ToString());
                }
            }
        }
        
        
        public void Write(Dictionary<string, string> qp)
        {
            if (qp == null)
                return;
            foreach (var kvp in qp)
            {
                Write(kvp.Key);
                Write(kvp.Value);
            }
        }

        public void Write(IReadOnlyList<Facet> facets)
        {
            if (facets == null)
                return;
            for (int i = 0; i < facets.Count; i++)
            {
                var facet = facets[i];
                Write(facet.AggregationField);
                Write(facet.AggregationType);
                Write(facet.DisplayName);
                Write(facet.IncludeRemainingTerms);
                Write(facet.MaxResults);
                Write(facet.Name);
                Write((int)facet.Aggregation);
                Write((int)facet.Mode);
                Write((int)facet.TermSortMode);
                Write(facet.Ranges);
            }
        }
    }
}