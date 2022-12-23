from utils import native_types, capital_type_map


class MainSorting:
    def __init__(self, configuration):
        self.namespace = configuration.namespace

    def generate_prologue(self, f):
        g = self
        s = F"""
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace {g.namespace}
{{
    partial class Sort
    {{
        [SkipLocalsInit]
        public unsafe static void Run<T>(T* left, T* right) where T : unmanaged
        {{ 
        """
        print(s, file=f)

    def generate_public_api(self, f):
        g = self
        s = ""
        for t in native_types:
            s += F"""
            if (typeof(T) == typeof({t}))
            {{
                {t}* il = ({t}*)left;
                {t}* ir = ({t}*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert({g.get_configuration_constant(t, "Unroll")} >= 1);
                Debug.Assert({g.get_configuration_constant(t, "Unroll")} <= 12);
                
                if (length < {g.get_configuration_constant(t, "SmallSortThresholdElements")})
                {{
                    BitonicSort.Sort(il, (int)length);
                    return;
                }}
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[{g.get_configuration_constant(t, "PartitionTempSizeInBytes")}];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, {t}.MinValue, {t}.MaxValue, {g.get_configuration_constant(t, "REALIGN_BOTH")}, depthLimit);
                return;
            }}            
"""
        s += F"""
            throw new NotSupportedException($"The current type {{typeof(T).Name}} is not supported by this method.");"""

        print(s, file=f)

    def generate_epilogue(self, f):
        s = F"""
        }}
    }}
}}
"""
        print(s, file=f)

    def get_configuration_constant(self, type, param):
        return F"Avx2VectorizedSort.{capital_type_map[type]}Config.{param}"
