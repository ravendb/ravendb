using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Storage.Managed.Data
{
	public class Tree
	{
		private readonly Stream reader;
		private readonly Stream writer;
		private readonly BinaryReaderWith7BitEncoding binaryReader;
		private readonly BinaryWriterWith7BitEncoding binaryWriter;
		private TreeNode root;

		public long RootPosition { get; private set; }

		public IEnumerable<JToken> Keys
		{
			get
			{
				if (root.NodeKey == null)
					yield break;
				foreach (var treeNode in root.IndexScan())
				{
					yield return treeNode.NodeKey;
				}
			}
		}

		private readonly List<Tuple<StreamPosition, TreeNode>> unwritten = new List<Tuple<StreamPosition, TreeNode>>();

		public Tree(Stream reader, Stream writer, StartMode mode)
		{
			this.reader = reader;
			this.writer = writer;
			binaryReader = new BinaryReaderWith7BitEncoding(this.reader);
			binaryWriter = new BinaryWriterWith7BitEncoding(this.writer);

			root = mode == StartMode.Create
				? new TreeNode(null, null, null, null, ReadNode, WriteNodeLazy)
				: ReadNode(new StreamPosition(reader.Position, null));
		}

		private StreamPosition WriteNodeLazy(TreeNode arg)
		{
			if (arg == null)
				return null;

			var positionInFile = new StreamPosition(arg);
			unwritten.Add(new Tuple<StreamPosition, TreeNode>(positionInFile, arg));
			return positionInFile;
		}

		private long WriteNode(TreeNode arg)
		{
			var position = writer.Position;
			Write(arg.NodeKey);
			binaryWriter.WriteBitEncodedNullableInt64(arg.Left == null ? null : (arg.Left.Position));
			binaryWriter.WriteBitEncodedNullableInt64(arg.Right == null ? null : (arg.Right.Position));
			binaryWriter.WriteBitEncodedNullableInt64(arg.NodeValue);
			return position;
		}

		private TreeNode ReadNode(StreamPosition streamPosition)
		{
			if (streamPosition.Node != null)
				return streamPosition.Node;
			if (streamPosition.Position == null)
				throw new InvalidOperationException("Cannot read an unwritten node");

			reader.Position = streamPosition.Position.Value;

			return new TreeNode(
				ReadJToken(),
				ReadPositionInFile(),
				ReadPositionInFile(),
				binaryReader.ReadBitEncodedNullableInt64(),
				ReadNode,
				WriteNodeLazy);
		}

		private StreamPosition ReadPositionInFile()
		{
			var l = binaryReader.ReadBitEncodedNullableInt64();
			if (l == null)
				return null;
			return new StreamPosition(l.Value, null);
		}

		private JToken ReadJToken()
		{
			var tokenType = (JTokenType)reader.ReadByte();
			switch (tokenType)
			{
				case JTokenType.None:
				case JTokenType.Constructor:
				case JTokenType.Comment:
				case JTokenType.Raw:
				case JTokenType.Property:
					throw new NotSupportedException("Can't read " + tokenType);
				case JTokenType.Object:
				case JTokenType.Array:
					return JToken.ReadFrom(new BsonReader(reader));
				case JTokenType.Integer:
					return binaryReader.ReadInt32();
				case JTokenType.Float:
					return binaryReader.ReadSingle();
				case JTokenType.String:
					return binaryReader.ReadString();
				case JTokenType.Boolean:
					return binaryReader.ReadBoolean();
				case JTokenType.Null:
				case JTokenType.Undefined:
					return new JValue((object)null);
				case JTokenType.Date:
					return DateTime.FromBinary(binaryReader.ReadInt64());
				case JTokenType.Bytes:
					var len = binaryReader.ReadInt32();
					return binaryReader.ReadBytes(len);
				default:
					throw new ArgumentOutOfRangeException();
			}
		}


		private void Write(JToken token)
		{
			if(token == null)
			{
				binaryWriter.Write((byte)JTokenType.Null);
				return;
			}
			binaryWriter.Write((byte)token.Type);
			switch (token.Type)
			{
				case JTokenType.None:
				case JTokenType.Constructor:
				case JTokenType.Comment:
				case JTokenType.Raw:
				case JTokenType.Property:
					throw new NotSupportedException("Can't write " + token);
				case JTokenType.Object:
				case JTokenType.Array:
					token.WriteTo(new BsonWriter(writer));
					break;
				case JTokenType.Integer:
					binaryWriter.Write(token.Value<int>());
					break;
				case JTokenType.Float:
					binaryWriter.Write(token.Value<float>());
					break;
				case JTokenType.String:
					binaryWriter.Write(token.Value<string>());
					break;
				case JTokenType.Boolean:
					binaryWriter.Write(token.Value<bool>());
					break;
				case JTokenType.Null:
				case JTokenType.Undefined:
					break;
				case JTokenType.Date:
					binaryWriter.Write(token.Value<DateTime>().ToBinary());
					break;
				case JTokenType.Bytes:
					var buffer = token.Value<byte[]>();
					binaryWriter.Write(buffer.Length);
					binaryWriter.Write(buffer);
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public void Add(JToken key, long documentPosition)
		{
			root = root.Add(key, documentPosition);
		}

		public long? FindValue(JToken key)
		{
			var node =  FindNode(key);
			return node == null ? null : node.NodeValue;
		}

		public TreeNode FindNode(JToken key)
		{
			if(root.CanSeek(key) == false)
			{
				var node = root.IndexScan()
					.FirstOrDefault(x=>TreeNode.Comparer.Compare(key, x.NodeKey) == 0);
				return node;
			}
			TreeNode found;
			if (root.IndexSeek(key, out found) == false)
				return null;
			return found;
		}

		public void Flush()
		{
			foreach (var tuple in unwritten)
			{
				if(tuple.Item2 == root)
					continue;
				var streamPosition = WriteNode(tuple.Item2);
				tuple.Item1.Position = streamPosition;
			}
			unwritten.Clear();
			RootPosition = WriteNode(root);
		}

		public void Remove(JToken key)
		{
			root = root.Remove(key, node => unwritten.RemoveAll(tuple => tuple.Item2 == node))
				// it may be that we delete the last item
				?? new TreeNode(null, null, null, null, ReadNode, WriteNodeLazy);
		}

		public JToken GetLeftMost()
		{
			return root.GetLeftMost().NodeKey;
		}

		public JToken GetRightMost()
		{
			return root.GetRightMost().NodeKey;
		}

		public IEnumerable<TreeNode> ScanFromExclusive(JToken key)
		{
			return root.IndexScanGreaterThan(key);
		}

		public IEnumerable<TreeNode> ScanFromInclusive(JToken key)
		{
			return root.IndexScanGreaterThanOrEqual(key);
		}

		public IEnumerable<TreeNode> ReverseScanFromInclusive(JToken key)
		{
			return root.ReverseIndexScanGreaterThanOrEqual(key);
		}

		public IEnumerable<TreeNode> IndexScan()
		{
			return root.IndexScan();
		}

	}
}