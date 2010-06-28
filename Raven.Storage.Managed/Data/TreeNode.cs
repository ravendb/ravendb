using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Storage.Managed.Data
{
	public class TreeNode
	{
		public static readonly JTokenComparer Comparer = new JTokenComparer();
		public readonly StreamPosition Left;
		public readonly JToken NodeKey;

		public readonly StreamPosition Right;

		public readonly long? NodeValue;

		private readonly Func<StreamPosition, TreeNode> readNode;
		private readonly Func<TreeNode, StreamPosition> writeNode;

		public TreeNode(JToken key, StreamPosition left, StreamPosition right, long? value, Func<StreamPosition, TreeNode> readNode, Func<TreeNode, StreamPosition> writeNode)
		{
			NodeKey = key;
			Left = left;
			Right = right;
			NodeValue = value;
			this.readNode = readNode;
			this.writeNode = writeNode;
		}

		public TreeNode Add(JToken key, long value)
		{
			if (ReferenceEquals(key, null))
				throw new ArgumentNullException("key");

			if (NodeKey == null || NodeKey.Type == JTokenType.Null) //root
				return new TreeNode(key, null, null, value, readNode, writeNode);

			var comparision = Comparer.Compare(key, NodeKey);

			var right = Right;
			var left = Left;

			if (comparision == 0) // there is no need to balance here
				return new TreeNode(key, Left, Right, value, readNode, writeNode);

			if (comparision > 0)
			{
				right = writeNode(CreateOrAddNode(key, value, right).Balance());
			}
			if (comparision < 0)
			{
				left = writeNode(CreateOrAddNode(key, value, left).Balance());
			}
			return new TreeNode(
				NodeKey,
				left,
				right,
				NodeValue,
				readNode,
				writeNode
				).Balance();
		}

		private TreeNode CreateOrAddNode(JToken key, long value, StreamPosition nodePos)
		{
			if (nodePos == null)
				return new TreeNode(key, null, null, value, readNode, writeNode);
			var node = readNode(nodePos);
			return node.Add(key, value);
		}

		private TreeNode Balance()
		{
			switch (Difference)
			{
				case 0:
				case 1:
				case -1:
					return this;
				case 2:
					var rightDiff = readNode(Right).Difference;
					if (rightDiff >= 0)
						return PivotLeft();
					var right = readNode(Right);
					var rightNodeRightRotated = new TreeNode(
						NodeKey,
						Left,
						writeNode(right.PivotRight()),
						NodeValue,
						readNode,
						writeNode
						);
					return rightNodeRightRotated.PivotLeft();
				case -2:
					var leftDiff = readNode(Left).Difference;
					if (leftDiff <= 0)
						return PivotRight();
					var left = readNode(Left);
					var leftNodeLeftRotated = new TreeNode(
						NodeKey,
						writeNode(left.PivotLeft()),
						Right,
						NodeValue,
						readNode,
						writeNode
						);
					return leftNodeLeftRotated.PivotRight();
				default:
					throw new InvalidOperationException();
			}
		}

		private TreeNode PivotLeft()
		{
			var pivot = readNode(Right);
			var rootToChildNode = new TreeNode(
				NodeKey,
				pivot.Left,
				Left,
				NodeValue,
				readNode,
				writeNode
				);

			return new TreeNode(
				pivot.NodeKey,
				writeNode(rootToChildNode),
				pivot.Right,
				pivot.NodeValue,
				readNode,
				writeNode
				);
		}

		private TreeNode PivotRight()
		{
			var pivot = readNode(Left);
			var rootToChildNode = new TreeNode(
				NodeKey,
				pivot.Right,
				Right,
				NodeValue,
				readNode,
				writeNode
				);

			return new TreeNode(
				pivot.NodeKey,
				pivot.Left,
				writeNode(rootToChildNode),
				pivot.NodeValue,
				readNode,
				writeNode
				);
		}

		private int Difference
		{
			get
			{
				var rightHeight = Right != null ? readNode(Right).Height + 1 : 0;
				var leftHeight = Left != null ? readNode(Left).Height + 1 : 0;

				return rightHeight - leftHeight;
			}
		}

		public int Height
		{
			get
			{
				var myHieght = (Left ?? Right) != null ? 1 : 0;
				return myHieght + Math.Max(Right != null ? readNode(Right).Height : 0, Left != null ? readNode(Left).Height : 0);
			}
		}

		public bool IndexSeek(JToken key, out TreeNode value)
		{
			if (ReferenceEquals(key, null))
				throw new ArgumentNullException("key");

			value = null;

			var comparision = Comparer.Compare(key, NodeKey);
			if (comparision > 0)
			{
				return Right != null && readNode(Right).IndexSeek(key, out value);
			}
			if (comparision < 0)
			{
				return Left != null && readNode(Left).IndexSeek(key, out value);
			}
			value = this;
			return true;
		}

		public TreeNode Remove(JToken key, Action<TreeNode> onRemovedNode)
		{
			if (ReferenceEquals(key, null))
				throw new ArgumentNullException("key");

			if (NodeKey == null)
				return this;

			var comparision = Comparer.Compare(key, NodeKey);

			if (comparision == 0)
			{
				if (Right != null && Left != null)
				{
					onRemovedNode(this);
					var node = readNode(Right).GetLeftMost();
					return new TreeNode(
						node.NodeKey,
						Left,
						writeNode(node.Remove(node.NodeKey, onRemovedNode)),
						node.NodeValue,
						readNode,
						writeNode
						).Balance();
				}
				var eitherSide = Right ?? Left;
				if (eitherSide != null)
					return readNode(eitherSide);
				return null;
			}

			if (comparision > 0 && Right != null)
			{
				return new TreeNode(NodeKey, Left, writeNode(readNode(Right).Remove(key, onRemovedNode)), NodeValue, readNode, writeNode).Balance();
			}
			if (comparision < 0 && Left != null)
			{
				return new TreeNode(NodeKey, writeNode(readNode(Left).Remove(key, onRemovedNode)), Right, NodeValue, readNode, writeNode).Balance();

			}

			return this;
		}

		public TreeNode GetLeftMost()
		{
			var left = this;
			while (left.Left != null)
			{
				left = readNode(left.Left);
			}
			return left;
		}

		public TreeNode GetRightMost()
		{
			var right = this;
			while (right.Right != null)
			{
				right = readNode(right.Right);
			}
			return right;
		}


		public IEnumerable<TreeNode> IndexScanGreaterThanOrEqual(JToken key)
		{
			return IndexScan(node => Comparer.Compare(key, node.NodeKey) <= 0);
		}

		public IEnumerable<TreeNode> IndexScanGreaterThan(JToken key)
		{
			return IndexScan(node => Comparer.Compare(key, node.NodeKey) < 0);
		}

		public IEnumerable<TreeNode> IndexScan()
		{
			return IndexScan(null);
		}

		private static bool NotNullNode(TreeNode node)
		{
			return node.NodeKey != null && node.NodeKey.Type != JTokenType.Null;
		}

		private IEnumerable<TreeNode> IndexScan(Predicate<TreeNode> predicate)
		{
			if (Left != null)
			{
				var node = readNode(Left);
				if (NotNullNode(node) && (predicate == null || predicate(node)))
				{
					foreach (var treeNode in node.IndexScan(predicate))
					{
						yield return treeNode;
					}
				}
			}
			if (NotNullNode(this) && (predicate == null || predicate(this)))
				yield return this;
			if (Right != null)
			{
				var node = readNode(Right);
				if (NotNullNode(node) && (predicate == null || predicate(node)))
				{
					foreach (var treeNode in node.IndexScan(predicate))
					{
						yield return treeNode;
					}
				}
			}
		}

		private IEnumerable<TreeNode> ReverseIndexScan(Predicate<TreeNode> predicate)
		{
			if (Right != null)
			{
				var node = readNode(Right);
				if (predicate(node))
				{
					foreach (var treeNode in node.IndexScan(predicate))
					{
						yield return treeNode;
					}
				}
			} 
			if (predicate(this))
				yield return this;
			if (Left != null)
			{
				var node = readNode(Left);
				if (predicate(node))
				{
					foreach (var treeNode in node.IndexScan(predicate))
					{
						yield return treeNode;
					}
				}
			}
			
		}

		// We can seek if the value is a complete match
		// to the schema of the node key (assuming that all nodes keys share the same schema)
		// Or if the key to search is a partial (left) match 
		public bool CanSeek(JToken key)
		{
			if (NodeKey == null)
				return false;

			if (key.Type != NodeKey.Type)
				return false;


			switch (key.Type)
			{
				case JTokenType.Object:
					// need to ensure that all the properties are on the left side
					var nodeKeyAsObj = ((JObject)NodeKey);
					var keyAsObj = ((JObject)key);
					var keyPropCount = keyAsObj.Properties().Count();
					var matchedCount = 0;
					foreach (var prop in nodeKeyAsObj)
					{
						JToken _;
						if (keyAsObj.TryGetValue(prop.Key, out _) == false)
							return matchedCount == keyPropCount;
						matchedCount++;
					}
					return matchedCount == keyAsObj.Count;
				default:
					return true;
			}
		}

		public IEnumerable<TreeNode> ReverseIndexScanGreaterThanOrEqual(JToken key)
		{
			return ReverseIndexScan(node => Comparer.Compare(key, node.NodeKey) <= 0);			
		}
	}
}