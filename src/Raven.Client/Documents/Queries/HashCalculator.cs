using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Raven.Client.Extensions;
using Raven.Client.Util;
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

        public void Write(ulong ul)
        {
            _buffer.Write((byte*)&ul, sizeof(ulong));
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

        public void Write(uint ui)
        {
            _buffer.Write((byte*)&ui, sizeof(uint));
        }

        public void Write(short sh)
        {
            _buffer.Write((byte*)&sh, sizeof(short));
        }

        public void Write(ushort ush)
        {
            _buffer.Write((byte*)&ush, sizeof(ushort));
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

        public void Write(sbyte sby)
        {
            _buffer.Write((byte*)&sby, sizeof(sbyte));
        }

        public void Write(char ch)
        {
            _buffer.Write((byte*)&ch, sizeof(char));
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
                WriteParameterValue(kvp.Value, 100);
            }
        }

        private void WriteParameterValue(object value, int level)
        {
            if (level <= 0)
            {
                if (value is null)
                    _buffer.WriteByte(0);
                else
                    Write(value.ToString());

                return;
            }

            switch (value)
            {
                case string s:
                    Write(s);
                    break;

                case char ch:
                    Write(ch);
                    break;

                case long l:
                    Write(l);
                    break;

                case ulong ul:
                    Write(ul);
                    break;

                case int i:
                    Write(i);
                    break;

                case uint ui:
                    Write(ui);
                    break;

                case short sh:
                    Write(sh);
                    break;

                case ushort ush:
                    Write(ush);
                    break;

                case bool b:
                    Write(b);
                    break;

                case double d:
                    Write(d.ToString("R"));
                    break;

                case float f:
                    Write(f.ToString("R"));
                    break;

                case decimal dec:
                    Write(dec.ToString("G"));
                    break;

                case null:
                    _buffer.WriteByte(0);
                    break;

                case DateTime dt:
                    Write(dt.ToString("o"));
                    break;

                case DateTimeOffset dto:
                    Write(dto.ToString("o"));
                    break;

                case TimeSpan ts:
                    Write(ts.ToString("c"));
                    break;

                case Guid guid:
                    Write(guid.ToString());
                    break;

                case byte bt:
                    _buffer.WriteByte(bt);
                    break;

                case sbyte sbt:
                    Write(sbt);
                    break;

                case Enum enm:
                    Write(enm.ToString());
                    break;

                case IDictionary dict:
                    bool hadDictionaryValues = false;
                    var dictionaryEnumerator = dict.GetEnumerator();
                    while (dictionaryEnumerator.MoveNext())
                    {
                        WriteParameterValue(dictionaryEnumerator.Key, level - 1);
                        WriteParameterValue(dictionaryEnumerator.Value, level - 1);
                        hadDictionaryValues = true;
                    }
                    if (hadDictionaryValues == false)
                    {
                        Write("empty-dictionary");
                    }
                    break;

                case IEnumerable e:
                    bool hadEnumerableValues = false;
                    var enumerator = e.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        WriteParameterValue(enumerator.Current, level - 1);
                        hadEnumerableValues = true;
                    }
                    if (hadEnumerableValues == false)
                    {
                        Write("empty-enumerator");
                    }

                    break;

                default:

                    bool hasObjectValues = false;
                    foreach (var memberInfo in ReflectionUtil.GetPropertiesAndFieldsFor(value.GetType(), BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                    {
                        WriteParameterValue(memberInfo.GetValue(value), level - 1);
                        hasObjectValues = true;
                    }
                    if (hasObjectValues == false)
                    {
                        Write("empty-object");
                    }

                    Write(value.ToString());

                    break;
            }
        }
    }
}
