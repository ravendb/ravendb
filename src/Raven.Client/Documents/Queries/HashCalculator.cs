using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    public unsafe struct HashCalculator : IDisposable
    {
        private UnmanagedWriteBuffer _buffer;

        public HashCalculator(JsonOperationContext ctx)
        {
            _buffer = ctx.GetStream(JsonOperationContext.InitialStreamSize);
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
            if (l != null)
                Write(l.Value);
            else
                Write("null-long");
        }

        public void Write(float? f)
        {
            if (f != null)
                Write(f.Value);
            else
                Write("null-float");
        }

        public void Write(int? i)
        {
            if (i != null)
                Write(i.Value);
            else
                Write("null-int");
        }

        public void Write(int i)
        {
            _buffer.Write((byte*)&i, sizeof(int));
        }

        public void Write(bool b)
        {
            _buffer.WriteByte(b ? (byte)1 : (byte)2);
        }

        public void Write(bool? b)
        {
            if (b != null)
                _buffer.WriteByte(b.Value ? (byte)1 : (byte)2);
            else
                Write("null-bool");
        }

        public void Write(string s)
        {
            if (s == null)
            {
                Write("null-string");
                return;
            }
            fixed (char* pQ = s)
                _buffer.Write((byte*)pQ, s.Length * sizeof(char));
        }

        public void Write(string[] s)
        {
            if (s == null)
            {
                Write("null-str-array");
                return;
            }
            Write(s.Length);
            for (int i = 0; i < s.Length; i++)
            {
                Write(s[i]);
            }
        }

        public void Write(List<string> s)
        {
            if (s == null)
            {
                Write("null-list-str");
                return;
            }
            Write(s.Count);
            for (int i = 0; i < s.Count; i++)
            {
                Write(s[i]);
            }
        }

        public void Dispose()
        {
            _buffer.Dispose();
        }

        public void Write(Parameters qp)
        {
            if (qp == null)
            {
                Write("null-params");
                return;
            }
            Write(qp.Count);
            foreach (var kvp in qp)
            {
                Write(kvp.Key);
                WriteParameterValue(kvp.Value);
            }
        }

        private void WriteParameterValue(object value)
        {
            if (value is string s)
            {
                Write(s);
            }
            else if (value is long l)
            {
                Write(l);
            }
            else if (value is int i)
            {
                Write(i);
            }
            else if (value is bool b)
            {
                Write(b);
            }
            else if (value is double d)
            {
                Write(d.ToString("R"));
            }
            else if (value is float f)
            {
                Write(f.ToString("R"));
            }
            else if (value == null)
            {
                _buffer.WriteByte(0);
            }
            else if (value is DateTimeOffset dto)
            {
                Write(dto.ToString("o"));
            }
            else if (value is IEnumerable e)
            {
                bool hadValues = false;
                var enumerator = e.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    WriteParameterValue(enumerator.Current);
                    hadValues = true;
                }
                if (hadValues == false)
                {
                    Write("empty-enumerator");
                }
            }
            else
            {
                Write(value.ToString());
            }
        }


        public void Write(Dictionary<string, string> qp)
        {
            if (qp == null)
            {
                Write("null-dic<string,string>");
                return;
            }
            Write(qp.Count);
            foreach (var kvp in qp)
            {
                Write(kvp.Key);
                Write(kvp.Value);
            }
        }
    }
}
