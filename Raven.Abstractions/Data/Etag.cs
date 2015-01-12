using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Linq;
using System.Security.Cryptography;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
	[Serializable]
	public class Etag : IEquatable<Etag>, IComparable<Etag>
	{

		public override int GetHashCode()
		{
			unchecked
			{
				return (restarts.GetHashCode() * 397) ^ changes.GetHashCode();
			}
		}

		long restarts;
		long changes;

		public long Restarts
		{
			get { return restarts; }
		}
		public long Changes
		{
			get { return changes; }
		}

		public Etag()
		{

		}

		public Etag(string str)
		{
			var etag = Parse(str);
			restarts = etag.restarts;
			changes = etag.changes;
		}

		public Etag(UuidType type, long restarts, long changes)
		{
			this.restarts = ((long)type << 56) | restarts;
			this.changes = changes;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((Etag)obj);
		}

		public static bool operator ==(Etag a, Etag b)
		{
			if (ReferenceEquals(a, null) && ReferenceEquals(b, null))
				return true;
			if (ReferenceEquals(a, null))
				return false;
			return a.Equals(b);
		}

		public static bool operator !=(Etag a, Etag b)
		{
			return !(a == b);
		}

		public bool Equals(Etag other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return restarts == other.restarts && changes == other.changes;
		}

		public int CompareTo(Etag other)
		{
			if (ReferenceEquals(other, null))
				return -1;
			var sub = restarts - other.restarts;
			if (Math.Abs(sub) > 0)
				return sub > 0 ? 1 : -1;
			sub = changes - other.changes;
			if (sub != 0)
				return sub > 0 ? 1 : -1;
			return 0;
		}

		private IEnumerable<byte> ToBytes()
		{
			return BitConverter.GetBytes(restarts).Reverse()
				.Concat(BitConverter.GetBytes(changes).Reverse());
		}

		public byte[] ToByteArray()
		{
			return ToBytes().ToArray();
		}

		public unsafe override string ToString()
		{
		    var sb = new StringBuilder(36);
		    var buffer = stackalloc byte[8];

		    *((long*) buffer) = restarts;

            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[7]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[6]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[5]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[4]]);
		    sb.Append('-');
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[3]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[2]]);
            sb.Append('-');
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[1]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[0]]);
            sb.Append('-');

            *((long*)buffer) = changes;

            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[7]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[6]]);
            sb.Append('-'); 
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[5]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[4]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[3]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[2]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[1]]);
            sb.Append(GenericUtil.ByteToHexAsStringLookup[buffer[0]]);

			var etagAsString = sb.ToString();
			Debug.Assert(etagAsString.Length == 36); //prevent stupid bugs if something is refactored

			return etagAsString;
		}

		public unsafe static Etag Parse(byte[] bytes)
		{
		    var etag = new Etag();
            fixed (byte* restarts = bytes)
            {
                int fst = (*restarts << 24) | (*(restarts + 1) << 16) | (*(restarts + 2) << 8) | (*(restarts + 3));
                int snd = (*(restarts + 4) << 24) | (*(restarts + 5) << 16) | (*(restarts + 6) << 8) | (*(restarts + 7));
                etag.restarts = (uint)snd | ((long)fst << 32);

                var changes = restarts + 8;

                fst = (*changes << 24) | (*(changes + 1) << 16) | (*(changes + 2) << 8) | (*(changes + 3));
                snd = (*(changes + 4) << 24) | (*(changes + 5) << 16) | (*(changes + 6) << 8) | (*(changes + 7));
                etag.changes = (uint)snd | ((long)fst << 32);
		    }

			return etag;
		}

		public static bool TryParse(string str, out Etag etag)
		{
			try
			{
				etag = Parse(str);
				return true;
			}
			catch (Exception)
			{
				etag = null;
				return false;
			}
		}

		public static Etag Parse(string str)
		{
			if (string.IsNullOrEmpty(str))
				throw new ArgumentException("str cannot be empty or null");
			if (str.Length != 36)
				throw new ArgumentException(string.Format("str must be 36 characters. Perhaps you are trying to parse non-etag as etag? (string that was passed into Etag::Parse is {0})", str));

			var buffer = new byte[16]
			{
				byte.Parse(str.Substring(16, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(14, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(11, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(9, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(6, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(4, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(2, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(0, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(34, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(32, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(30, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(28, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(26, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(24, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(21, 2), NumberStyles.HexNumber),
				byte.Parse(str.Substring(19, 2), NumberStyles.HexNumber)
			};

			return new Etag
			{
				restarts = BitConverter.ToInt64(buffer, 0),
				changes = BitConverter.ToInt64(buffer, 8)
			};
		}

		public static Etag InvalidEtag
		{
			get
			{
				return new Etag
				{
					restarts = -1,
					changes = -1
				};
			}
		}
		public static Etag Empty
		{
			get
			{
				return new Etag
					{
						restarts = 0,
						changes = 0
					};
			}
		}

		public Etag Setup(UuidType type, long restartsNum)
		{
			return new Etag
				{
					restarts = ((long)type << 56) | restartsNum,
					changes = changes
				};
		}

		public Etag IncrementBy(int amount)
		{
			return new Etag
			{
				restarts = restarts,
				changes = changes + amount
			};
		}

		public static implicit operator string(Etag etag)
		{
			if (etag == null)
				return null;
			return etag.ToString();
		}

		public static implicit operator Etag(string s)
		{
			return Parse(s);
		}

		public static implicit operator Etag(Guid g)
		{
			return GuidToEtag(g);
		}

		public static implicit operator Guid?(Etag e)
		{
			if (e == null)
				return null;
			return EtagToGuid(e);
		}

		public static implicit operator Etag(Guid? g)
		{
			if (g == null)
				return null;
			return GuidToEtag(g.Value);
		}

		public static implicit operator Guid(Etag e)
		{
			return EtagToGuid(e);
		}

		public Etag HashWith(Etag other)
		{
			return HashWith(other.ToBytes());
		}

		public Etag HashWith(IEnumerable<byte> bytes)
		{
			var etagBytes = ToBytes().Concat(bytes).ToArray();

			return Parse(Encryptor.Current.Hash.Compute16(etagBytes));
		}

		public static Etag Max(Etag first, Etag second)
		{
			if (first == null)
				return second;
			return first.CompareTo(second) > 0
				? first
				: second;
		}

		private static Guid EtagToGuid(Etag etag)
		{
			var bytes = etag.ToByteArray();
			if (BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes, 0, 4);
				Array.Reverse(bytes, 4, 2);
				Array.Reverse(bytes, 6, 2);
			}
			return new Guid(bytes);
		}

		private static Etag GuidToEtag(Guid guid)
		{
			var bytes = guid.ToByteArray();
			if (BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes, 0, 4);
				Array.Reverse(bytes, 4, 2);
				Array.Reverse(bytes, 6, 2);
			}
			return Etag.Parse(bytes);
		}
	}

	public class EtagJsonConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			var etag = value as Etag;
			if (etag == null)
				writer.WriteNull();
			else
				writer.WriteValue(etag.ToString());
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var s = reader.Value as string;
			if (s == null)
				return null;
			return Etag.Parse(s);
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(Etag);
		}
	}
}
