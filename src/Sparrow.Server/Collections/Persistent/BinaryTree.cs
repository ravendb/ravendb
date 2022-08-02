using System;
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
                get { return (ushort)(_leftChild & ValueMask); }
                set
                {
                    int aux = (_leftChild & HasValueMask);
                    _leftChild = (ushort)((value & ValueMask) | aux);
                }
            }

            public ushort RightChild
            {
                get { return (ushort)(_rightChild & ValueMask); }
                set
                {
                    int aux = (_rightChild & HasValueMask);
                    _rightChild = (ushort)((value & ValueMask) | aux);
                }
            }

            public bool HasValue
            {
                get { return (_leftChild & HasValueMask) != 0; }
                set { _leftChild = (ushort)(Convert.ToInt32(value) << 15 | LeftChild); }
            } 
        }

        private readonly Span<byte> _storage;
        private readonly Span<Node> Nodes => MemoryMarshal.Cast<byte, Node>(_storage[2..]);

        private ushort FreeNodes
        {
            get { return MemoryMarshal.Read<ushort>(_storage); }
            set { MemoryMarshal.Write(_storage, ref value); }
        }

        public int MaxNodes => Nodes.Length - 1;
        public int AvailableNodes => Nodes.Length - FreeNodes - 1;
        public int MemoryUsed => (MaxNodes - AvailableNodes) * Unsafe.SizeOf<Node>() + sizeof(ushort);

        public BinaryTree(Span<byte> storage)
        {
            _storage = storage;

            if (Nodes.Length > short.MaxValue)
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
            Nodes[0]._leftChild = Invalid;
            Nodes[0]._rightChild = Invalid;

            FreeNodes = 1;
        }

        public void Add(ref BitReader key, T value)
        {
            Span<Node> nodes = Nodes;

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

        public bool Find(ref BitReader key, out T value)
        {
            Span<Node> nodes = Nodes;

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

        public readonly bool FindCommonPrefix(ref BitReader key, out T value)
        {
            Span<Node> nodes = Nodes;

            ref Node u = ref nodes[0];

            while (!u.HasValue)
            {
                if (key.Length == 0)
                    break;

                var current = key.Read();
                if (current.IsSet)
                {
                    //Console.Write("R");
                    u = ref nodes[u.RightChild];
                }
                else
                {
                    //Console.Write("L");
                    u = ref nodes[u.LeftChild];
                }
            }

            //Console.Write($",{u.Value}");
            value = u.Value;
            return u.HasValue;
        }
    }
}
