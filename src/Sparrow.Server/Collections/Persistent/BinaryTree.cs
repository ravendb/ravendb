using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server.Binary;

namespace Sparrow.Server.Collections.Persistent
{
    public ref struct BinaryTree<T> where T : struct
    {
        private const int ValueMask = 0x7FFF;
        private const int HasValueMask = 0x8000;
        private const ushort Invalid = ValueMask;

        [StructLayout(LayoutKind.Sequential)]
        private struct Node
        {
            internal ushort _leftChild;
            internal ushort _rightChild;
            public T Value;

            public ushort LeftChild
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (ushort)(_leftChild & ValueMask); }
                set
                {
                    int aux = (_leftChild & HasValueMask);
                    _leftChild = (ushort)((value & ValueMask) | aux);
                }
            }

            public ushort RightChild
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (ushort)(_rightChild & ValueMask); }
                set
                {
                    int aux = (_rightChild & HasValueMask);
                    _rightChild = (ushort)((value & ValueMask) | aux);
                }
            }

            public bool HasValue
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (_leftChild & HasValueMask) != 0; }
                set { _leftChild = (ushort)(value.ToInt32() << 15 | LeftChild); }
            } 
        }

        private readonly Span<byte> _storage;
        private readonly Span<Node> _nodes;

        private ushort FreeNodes
        {
            get { return MemoryMarshal.Read<ushort>(_storage); }
            set { MemoryMarshal.Write(_storage, in value); }
        }

        public int MaxNodes => _nodes.Length - 1;
        public int AvailableNodes => _nodes.Length - FreeNodes - 1;
        public int MemoryUsed => (MaxNodes - AvailableNodes) * Unsafe.SizeOf<Node>() + sizeof(ushort);

        public BinaryTree(Span<byte> storage)
        {
            _storage = storage;
            _nodes = MemoryMarshal.Cast<byte, Node>(_storage[2..]);

            if (_nodes.Length > short.MaxValue)
            {
                // We are going to be using 1 bit from the Left Child
                // therefore the max amount of nodes available are
                // less than the ushort.
                _storage = _storage.Slice(0, short.MaxValue - 1);
            }
        }

        public static BinaryTree<T> Create(Span<byte> storage)
        {
            var tree = new BinaryTree<T>(storage);
            tree.Initialize();
            return tree;
        }

        public static BinaryTree<T> Open(Span<byte> storage)
        {
            return new (storage);
        }

        public void Initialize()
        {
            _nodes[0]._leftChild = Invalid;
            _nodes[0]._rightChild = Invalid;

            FreeNodes = 1;
        }

        public void Add(ref BitReader key, T value)
        {
            Span<Node> nodes = _nodes;

            ref Node u = ref nodes[0];

            while (key.Length != 0)
            {
                Bit b = key.Read();
                if (b.IsSet)
                {
                    if (u.RightChild == Invalid)
                    {
                        ref Node newNode = ref nodes[FreeNodes];
                        newNode._leftChild = Invalid;
                        newNode._rightChild = Invalid;

                        u.RightChild = FreeNodes;
                        FreeNodes++;
                    }
                    //Console.Write("R");
                    u = ref nodes[u.RightChild];
                }
                else
                {
                    if (u.LeftChild == Invalid)
                    {
                        ref Node newNode = ref nodes[FreeNodes];
                        newNode._leftChild = Invalid;
                        newNode._rightChild = Invalid;

                        u.LeftChild = FreeNodes;
                        FreeNodes++;
                    }

                    //Console.Write("L");
                    u = ref nodes[u.LeftChild];
                }
            }

            u.Value = value;
            u.HasValue = true;

            //Console.WriteLine($",{u.Value}");
        }

        public void Add(uint key, int length, T value)
        {
            Span<Node> nodes = _nodes;

            ref Node u = ref nodes[0];

            int mask = 1 << (length - 1);

            while (mask > 0)
            {
                // If the bit is set, then we are going to the right.
                if ((key & mask) > 0)
                {
                    if (u.RightChild == Invalid)
                    {
                        ref Node newNode = ref nodes[FreeNodes];
                        newNode._leftChild = Invalid;
                        newNode._rightChild = Invalid;

                        u.RightChild = FreeNodes;
                        FreeNodes++;
                    }
                    //Console.Write("R");
                    u = ref nodes[u.RightChild];
                }
                else
                {
                    if (u.LeftChild == Invalid)
                    {
                        ref Node newNode = ref nodes[FreeNodes];
                        newNode._leftChild = Invalid;
                        newNode._rightChild = Invalid;

                        u.LeftChild = FreeNodes;
                        FreeNodes++;
                    }

                    //Console.Write("L");
                    u = ref nodes[u.LeftChild];
                }
                mask >>= 1;
            }

            u.Value = value;
            u.HasValue = true;

            //Console.WriteLine($",{u.Value}");
        }
        public bool Find(ref BitReader key, out T value)
        {
            Span<Node> nodes = _nodes;

            ref Node u = ref nodes[0];

            while (key.Length != 0)
            {
                var current = key.Read();
                if (current.IsSet)
                {
                    if (u.RightChild == Invalid)
                    {
                        value = default;
                        return false;
                    }
                    u = ref nodes[u.RightChild];
                }
                else
                {
                    if (u.LeftChild == Invalid)
                    {
                        value = default;
                        return false;
                    }

                    u = ref nodes[u.LeftChild];
                }
            }


            value = u.Value;
            return u.HasValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int FindCommonPrefix(ref byte key, int lengthInBits, int currentBit, out T value)
        {
            ref Node nodeRef = ref MemoryMarshal.GetReference(_nodes);

            int keyLength = lengthInBits - currentBit;

            ref Node currentNode = ref nodeRef;
            while (!currentNode.HasValue)
            {
                // PERF: We want to always take to improve the predictability of the method.
                if (keyLength != 0)
                {
                    // We can easily find the the current byte by a simple shift, which is what divided by 8 will be translated to. 
                    ref byte currentByte = ref Unsafe.AddByteOffset(ref key, new IntPtr(currentBit / 8));

                    // PERF: There are 2 ways to do this. The first involve shifting the bits to leave the current bit as the last bit
                    // in the byte. However, for that we need to find the number of bits subtracting from the total amount of 
                    // of bits available. The alternative is to create a mask that we can use the output of the modulus directly 
                    // to position the mask in the right place. And then, if the value is different than 0, then we know the bit is set.
                    // In this way we just need 2 instructions to do so. 
                    var current = currentByte & (0b1000_0000 >> (currentBit % 8));

                    int u = current == 0 ? currentNode.LeftChild : currentNode.RightChild;
                    currentNode = ref Unsafe.AddByteOffset(ref nodeRef, new IntPtr(u * Unsafe.SizeOf<Node>()));

                    keyLength--;
                    currentBit++;
                    continue;
                }

                // If we haven't found a value in here, all this work was useless and we have an issue in the data stream. Therefore, 
                // we will return -1 which is a totally anomalous result.
                Unsafe.SkipInit(out value);
                return -1;
            }

            value = currentNode.Value;
            return currentBit;
        }

        public readonly bool FindCommonPrefix(ref BitReader key, out T value)
        {
            Span<Node> nodes = _nodes;

            int u = 0;
            while (!nodes[u].HasValue)
            {
                if (key.Length == 0)
                    break;

                var current = key.Read();

                u = current.IsSet ?
                    nodes[u].RightChild :
                    nodes[u].LeftChild;
            }

            value = nodes[u].Value;
            return nodes[u].HasValue;
        }
    }
}
