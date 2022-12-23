import os
from datetime import datetime

from utils import native_size_map, next_power_of_2
from bitonic_isa import BitonicISA


class AVX2BitonicISA(BitonicISA):
    def __init__(self, type, configuration):
        self.vector_size_in_bytes = 32

        self.type = type

        self.bitonic_size_map = {}

        for t, s in native_size_map.items():
            self.bitonic_size_map[t] = int(self.vector_size_in_bytes / s)

        self.bitonic_type_map = {
            "int": "Int32",
            "uint": "Int32",
            "float": "Int32",
            "long": "Int64",
            "ulong": "Int64",
            "double": "Int64",
        }

        self._max_bitonic_sort_vectors = configuration.max_bitonic_sort_vectors
        self.unroll_bitonic_sorters = configuration.unroll_bitonic_sorters
        self.namespace = configuration.namespace

        self._var_names = [f"d{i:02d}" for i in range(0, self.max_bitonic_sort_vectors + 2)]

    @property
    def max_bitonic_sort_vectors(self):
        return self._max_bitonic_sort_vectors

    def vector_size(self):
        return self.bitonic_size_map[self.type]

    def vector_type(self):
        return "V"

    @classmethod
    def supported_types(cls):
        return native_size_map.keys()

    def i2d(self, v):
        t = self.type
        if t == "double":
            return v
        return f"Vector256.AsDouble({v})"

    def i2s(self, v):
        t = self.type
        if t == "double":
            raise Exception("Incorrect Type")
        elif t == "float":
            return f"Vector256.AsSingle({v})"
        return v

    def d2i(self, v):
        t = self.type
        if t == "float":
            return f"Vector256.AsSingle({v})"
        elif t == "int":
            return f"Vector256.AsInt32({v})"
        elif t == "uint":
            return f"Vector256.AsUInt32({v})"
        elif t == "long":
            return f"Vector256.AsInt64({v})"
        elif t == "ulong":
            return f"Vector256.AsUInt64({v})"

        return v

    def s2i(self, v):
        t = self.type
        if t == "double":
            raise Exception("Incorrect Type")
        elif t == "float":
            return f"Vector256.AsInt32({v})"
        return v

    def generate_param_list(self, start, numParams):
        return str.join(", ", list(map(lambda p: f"ref d{p:02d}", range(start, start + numParams))))

    def generate_param_def_list(self, numParams):
        return str.join(", ", list(map(lambda p: f"ref {self.vector_type()} d{p:02d}", range(1, numParams + 1))))

    def generate_shuffle_X1(self, v):
        size = self.vector_size()
        if size == 8:
            return self.i2s(f"Shuffle({self.s2i(v)}, 0xB1)")
        elif size == 4:
            return self.d2i(f"Shuffle({self.i2d(v)}, {self.i2d(v)}, 0x5)")

    def generate_shuffle_X2(self, v):
        size = self.vector_size()
        if size == 8:
            return self.i2s(f"Shuffle({self.s2i(v)}, 0x4E)")
        elif size == 4:
            return self.d2i(f"Permute4x64({self.i2d(v)}, 0x4E)")

    def generate_shuffle_XR(self, v):
        size = self.vector_size()
        if size == 8:
            return self.i2s(f"Shuffle({self.s2i(v)}, 0x1B)")
        elif size == 4:
            return self.d2i(f"Permute4x64({self.i2d(v)}, 0x1B)")

    def generate_blend_B1(self, v1, v2, ascending):
        size = self.vector_size()
        if size == 8:
            if ascending:
                return self.i2s(f"Blend({self.s2i(v1)}, {self.s2i(v2)}, 0xAA)")
            else:
                return self.i2s(f"Blend({self.s2i(v2)}, {self.s2i(v1)}, 0xAA)")
        elif size == 4:
            if ascending:
                return self.d2i(f"Blend({self.i2d(v1)}, {self.i2d(v2)}, 0xA)")
            else:
                return self.d2i(f"Blend({self.i2d(v2)}, {self.i2d(v1)}, 0xA)")

    def generate_blend_B2(self, v1, v2, ascending):
        size = self.vector_size()
        if size == 8:
            if ascending:
                return self.i2s(f"Blend({self.s2i(v1)}, {self.s2i(v2)}, 0xCC)")
            else:
                return self.i2s(f"Blend({self.s2i(v2)}, {self.s2i(v1)}, 0xCC)")
        elif size == 4:
            if ascending:
                return self.d2i(f"Blend({self.i2d(v1)}, {self.i2d(v2)}, 0xC)")
            else:
                return self.d2i(f"Blend({self.i2d(v2)}, {self.i2d(v1)}, 0xC)")

    def generate_blend_B4(self, v1, v2, ascending):
        size = self.vector_size()
        if size == 8:
            if ascending:
                return self.i2s(f"Blend({self.s2i(v1)}, {self.s2i(v2)}, 0xF0)")
            else:
                return self.i2s(f"Blend({self.s2i(v2)}, {self.s2i(v1)}, 0xF0)")
        elif size == 4:
            raise Exception("Incorrect Size")

    def generate_cross(self, v):
        size = self.vector_size()
        if size == 8:
            return self.d2i(f"Permute4x64({self.i2d(v)}, 0x4E)")
        elif size == 4:
            raise Exception("Incorrect Size")

    def generate_reverse(self, v):
        size = self.vector_size()
        if size == 8:
            v = f"Shuffle({self.s2i(v)}, 0x1B)"
            return self.d2i(f"Permute4x64({self.i2d(v)}, 0x4E)")
        elif size == 4:
            return self.d2i(f"Permute4x64({self.i2d(v)}, 0x1B)")

    def crappity_crap_crap(self, v1, v2):
        t = self.type
        if t == "long":
            return f"cmp = CompareGreaterThan({v1}, {v2});"
        elif t == "ulong":
            return f"cmp = CompareGreaterThan(Xor(topBit, {v1}).AsInt64(), Xor(topBit, {v2}).AsInt64()).AsUInt64();"

        return ""

    def generate_min(self, v1, v2):
        t = self.type
        if t == "int":
            return f"Min({v1}, {v2})"
        elif t == "uint":
            return f"Min({v1}, {v2})"
        elif t == "float":
            return f"Min({v1}, {v2})"
        elif t == "long":
            return self.d2i(f"BlendVariable({self.i2d(v1)}, {self.i2d(v2)}, {self.i2d('cmp')})")
        elif t == "ulong":
            return self.d2i(f"BlendVariable({self.i2d(v1)}, {self.i2d(v2)}, {self.i2d('cmp')})")
        elif t == "double":
            return f"Min({v1}, {v2})"

    def generate_max(self, v1, v2):
        t = self.type
        if t == "int":
            return f"Max({v1}, {v2})"
        elif t == "uint":
            return f"Max({v1}, {v2})"
        elif t == "float":
            return f"Max({v1}, {v2})"
        elif t == "long":
            return self.d2i(f"BlendVariable({self.i2d(v2)}, {self.i2d(v1)}, {self.i2d('cmp')})")
        elif t == "ulong":
            return self.d2i(f"BlendVariable({self.i2d(v2)}, {self.i2d(v1)}, {self.i2d('cmp')})")
        elif t == "double":
            return f"Max({v1}, {v2})"

    def get_load_intrinsic(self, v, offset):
        t = self.type
        if t == "double":
            return f"LoadVector256({v} + V.Count * {offset})"
        if t == "float":
            return f"LoadVector256({v} + V.Count * {offset})"
        return f"LoadVector256({v} + V.Count * {offset})"

    def get_mask_load_intrinsic(self, v, offset, mask):
        t = self.type

        if self.vector_size() == 4:
            max_value = f"AndNot({mask}, Vector256.Create({t}.MaxValue))"
        elif self.vector_size() == 8:
            max_value = f"AndNot({mask}, Vector256.Create({t}.MaxValue))"

        if t == "double":
            max_value = f"AndNot(mask, Vector256.Create({t}.MaxValue))"
            load = f"MaskLoad({v} +  V.Count * {offset}, {mask})"
            return f"Or({load}, {max_value})"
        if t == "float":
            max_value = f"AndNot(mask, Vector256.Create({t}.MaxValue))"
            load = f"MaskLoad({v} +  V.Count * {offset}, {mask})"
            return f"Or({load}, {max_value})"

        load = f"MaskLoad({v} + V.Count * {offset}, {mask})"
        return f"Or({load}, {max_value})"

    def get_store_intrinsic(self, ptr, offset, value):
        t = self.type
        if t == "double":
            return f"Store(({t} *) ({ptr} +  V.Count * {offset}), {value})"
        if t == "float":
            return f"Store(({t} *) ({ptr} +  V.Count * {offset}), {value})"
        return f"Store({ptr} + V.Count * {offset}, {value})"

    def get_mask_store_intrinsic(self, ptr, offset, value, mask):
        t = self.type

        if t == "double":
            return f"MaskStore({ptr} +  V.Count * {offset}, {mask}, {value})"
        if t == "float":
            return f"MaskStore({ptr} +  V.Count * {offset}, {mask}, {value})"

        return f"MaskStore({ptr} +  V.Count * {offset}, {mask}, {value})"

    def autogenerated_blabber(self):
        return f"""/////////////////////////////////////////////////////////////////////////////
////
// This file was auto-generated by a tool at {datetime.now().strftime("%F %H:%M:%S")}
//
// It is recommended you DO NOT directly edit this file but instead edit
// the code-generator that generated this source file instead.
/////////////////////////////////////////////////////////////////////////////"""

    def generate_prologue(self, f):
        t = self.type
        g = self
        s = f"""{self.autogenerated_blabber()}

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using static System.Runtime.Intrinsics.X86.Avx;
using static System.Runtime.Intrinsics.X86.Avx2;
using static System.Runtime.Intrinsics.X86.Sse2;
using static System.Runtime.Intrinsics.X86.Sse41;
using static System.Runtime.Intrinsics.X86.Sse42;

namespace {g.namespace}
{{
    using V = Vector256<{self.type}>;
    static unsafe partial class BitonicSort
    {{
"""
        print(s, file=f)

    def generate_epilogue(self, f):
        s = f"""
    }};
}}
    """
        print(s, file=f)

    def generate_1v_sort(self, ascending, start=1):
        g = self
        type = self.type
        var = self._var_names
        maybe_cmp = lambda: ", cmp" if (type == "long" or type == "ulong") else ""
        maybe_topbit = lambda: f"{g.vector_type()} topBit = Vector256.Create(1UL << 63);" if type == "ulong" else ""

        s = f"""
            {g.vector_type()}  min, max, s{maybe_cmp()};
            {maybe_topbit()}

            s = {g.generate_shuffle_X1(var[start])};
            {g.crappity_crap_crap("s", var[start])}
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B1("min", "max", ascending)};

            s = {g.generate_shuffle_XR(var[start])};
            {g.crappity_crap_crap("s", var[start])}
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B2("min", "max", ascending)};

            s = {g.generate_shuffle_X1(var[start])};
            {g.crappity_crap_crap("s", var[start])}
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B1("min", "max", ascending)};"""

        if g.vector_size() == 8:
            s += f"""
            s = {g.generate_reverse(var[start])};
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B4("min", "max", ascending)};

            s = {g.generate_shuffle_X2(var[start])};
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B2("min", "max", ascending)};

            s = {g.generate_shuffle_X1(var[start])};
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B1("min", "max", ascending)};"""

        return s

    def generate_1v_basic_sorters(self, f, ascending):
        g = self
        suffix = "ascending" if ascending else "descending"

        s = f"""
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void sort_01v_{suffix}({g.generate_param_def_list(1)}) {{
            {self.generate_1v_sort(ascending, 1)}        
        }}"""

        print(s, file=f)

    def generate_1v_merge(self, ascending, start):
        g = self
        type = self.type
        var = self._var_names
        maybe_cmp = lambda: ", cmp" if (type == "long" or type == "ulong") else ""
        maybe_topbit = lambda: f"{g.vector_type()} topBit = Vector256.Create(1UL << 63);" if type == "ulong" else ""

        s = f"""
            {g.vector_type()}  min, max, s{maybe_cmp()};
            {maybe_topbit()}"""
        if g.vector_size() == 8:
            s += f"""
            s = {g.generate_cross(var[start])};
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B4("min", "max", ascending)};"""

        s += f"""
            s = {g.generate_shuffle_X2(var[start])};
            {g.crappity_crap_crap("s", var[start])}
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B2("min", "max", ascending)};

            s = {g.generate_shuffle_X1(var[start])};
            {g.crappity_crap_crap("s", var[start])}
            min = {g.generate_min("s", var[start])};
            max = {g.generate_max("s", var[start])};
            {var[start]} = {g.generate_blend_B1("min", "max", ascending)};"""

        return s

    def generate_1v_merge_sorters(self, f, ascending: bool):
        g = self
        suffix = "ascending" if ascending else "descending"

        s = f"""
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void sort_01v_merge_{suffix}({g.generate_param_def_list(1)}) {{
            {g.generate_1v_merge(ascending, 1)}
        }}
        """

        print(s, file=f)

    def generate_compounded_merge(self, width, ascending, start, numParams=1):
        g = self
        suffix = "ascending" if ascending else "descending"

        if width == 1:
            return f"""
        {{        
            //sort_{1:02d}v_merge_{suffix}({g.generate_param_list(1, start)});      
            {g.generate_1v_merge(ascending, start)}
        }}"""
        elif width < g.unroll_bitonic_sorters:
            return f"""
        {{
            //sort_{width:02d}v_merge_{suffix}({g.generate_param_list(start, numParams)}); 
            {g.generate_xv_compounded_merger(width, ascending, start)}
        }}"""
        else:
            return f"""
        {{
            //sort_{width:02d}v_merge_{suffix}({g.generate_param_list(start, numParams)});
            sort_{width:02d}v_merge_{suffix}({g.generate_param_list(start, numParams)});
        }}"""

    def generate_compounded_sort(self, width, ascending, start, numParams=1):
        g = self
        suffix = "ascending" if ascending else "descending"

        if width == 1:
            return f"""
        {{
            //sort_{1:02d}v_{suffix}({g.generate_param_list(1, start)});      
            {g.generate_1v_sort(ascending, start)}
        }}"""
        elif width < g.unroll_bitonic_sorters:
            return f"""
        {{
            //sort_{1:02d}v_{suffix}({g.generate_param_list(start, width)});
            {g.generate_xv_compounded_sort(width, ascending, start)}
        }}"""
        else:
            return f"""
        {{
            //sort_{width:02d}v_{suffix}({g.generate_param_list(start, numParams)});
            sort_{width:02d}v_{suffix}({g.generate_param_list(start, numParams)});
        }}"""

    def generate_xv_compounded_sort(self, width, ascending, start):
        type = self.type
        g = self
        var = self._var_names

        maybe_cmp = lambda: ", cmp" if (type == "long" or type == "ulong") else ""
        maybe_topbit = lambda: f"\n        {g.vector_type()} topBit = Vector256.Create(1UL << 63);" if (type == "ulong") else ""

        w1 = int(next_power_of_2(width) / 2)
        w2 = int(width - w1)

        s = f"""                 
            {g.generate_compounded_sort(w1, ascending, start, w1)}                        
            {g.generate_compounded_sort(w2, not ascending, w1 + start, w2)}"""

        ss = ""
        for r in range(w1 + start, width + start):
            x = w1 - (r - (w1 + start)) + (start - 1)
            ss += f"""
            tmp = {var[r]};
            {g.crappity_crap_crap(var[x], var[r])}
            {var[r]} = {g.generate_max(var[x], var[r])};
            {var[x]} = {g.generate_min(var[x], "tmp")};"""

        s += f"""            
        {{
            {g.vector_type()} tmp{maybe_cmp()};
            {maybe_topbit()}
            {ss}
        }}
                        
        {g.generate_compounded_merge(w1, ascending, start, w1)}                        
        {g.generate_compounded_merge(w2, ascending, w1 + start, w2)}"""

        return s

    def generate_compounded_sorter(self, f, width, ascending, inline):
        g = self
        suffix = "ascending" if ascending else "descending"
        inl = "AggressiveInlining" if inline else "NoInlining"

        s = f"""
    [MethodImpl(MethodImplOptions.{inl} | MethodImplOptions.AggressiveOptimization)]        
    private static void sort_{width:02d}v_{suffix}({g.generate_param_def_list(width)}) {{
        {g.generate_xv_compounded_sort(width, ascending, 1)}
    }}
"""
        print(s, file=f)

    def generate_xv_compounded_merger(self, width, ascending, start):
        type = self.type
        g = self
        var = self._var_names
        maybe_cmp = lambda: ", cmp" if (type == "long" or type == "ulong") else ""
        maybe_topbit = lambda: f"{g.vector_type()} topBit = Vector256.Create(1UL << 63);" if type == "ulong" else ""

        w1 = int(next_power_of_2(width) / 2)
        w2 = int(width - w1)

        s = F"""
        {{
        {g.vector_type()}  tmp{maybe_cmp()};
        {maybe_topbit()}"""

        for r in range(w1 + start, width + start):
            x = r - w1
            s += f"""
            tmp = {var[x]};
            {g.crappity_crap_crap(var[r], var[x])}
            d{x:02d} = {g.generate_min(var[r], var[x])};
            {g.crappity_crap_crap(var[r], "tmp")}
            d{r:02d} = {g.generate_max(var[r], "tmp")};"""

        s += f"""
        }}   
        
        {g.generate_compounded_merge(w1, ascending, start, w1)}                    
        {g.generate_compounded_merge(w2, ascending, w1 + start, w2)}   
"""

        return s

    def generate_compounded_merger(self, f, width, ascending, inline):
        g = self
        suffix = "ascending" if ascending else "descending"

        inl = "AggressiveInlining" if inline else "NoInlining"

        s = f"""
    [MethodImpl(MethodImplOptions.{inl} | MethodImplOptions.AggressiveOptimization)]        
    private static void sort_{width:02d}v_merge_{suffix}({g.generate_param_def_list(width)}) {{
        {g.generate_xv_compounded_merger(width, ascending, 1)}
    }}"""
        print(s, file=f)

    def generate_entry_points(self, f):
        type = self.type
        g = self
        var = self._var_names

        for m in range(1, g.max_bitonic_sort_vectors + 1):
            mask = f"""ConvertToVector256{self.bitonic_type_map[type]}(LoadVector128((sbyte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(mask_table_{self.vector_size()})) + remainder * V.Count))"""
            if type == "double":
                mask = f"Vector256.AsDouble({mask})"
            elif type == "float":
                mask = f"Vector256.AsSingle({mask})"
            elif type == 'uint':
                mask = f"""{mask}.AsUInt32()"""
            elif type == 'ulong':
                mask = f"""{mask}.AsUInt64()"""

            s = f"""
        [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.AggressiveOptimization)]
        private static void sort_{m:02d}v_alt({type} *ptr, int remainder) 
        {{        
            var mask = {mask};
"""
            print(s, file=f)

            for l in range(0, m-1):
                s = f"      {g.vector_type()} {var[l + 1]} = {g.get_load_intrinsic('ptr', l)};"
                print(s, file=f)

            s = f"      {g.vector_type()} {var[m]} = {g.get_mask_load_intrinsic('ptr', m - 1, 'mask')};"
            print(s, file=f)

            if m < g.unroll_bitonic_sorters:
                s = f"""
            // sort_{m:02d}v_ascending({g.generate_param_list(1, m)});
            {g.generate_compounded_sort(m, True, 1)}"""
            else:
                s = f"""
            sort_{m:02d}v_ascending({g.generate_param_list(1, m)});"""
            print(s, file=f)

            for l in range(0, m-1):
                s = f"      {g.get_store_intrinsic('ptr', l, var[l + 1])};"
                print(s, file=f)

            s = f"      {g.get_mask_store_intrinsic('ptr', m - 1, var[m], 'mask')};"
            print(s, file=f)

            print("     }", file=f)

    def generate_master_entry_point(self, f_header):

        def generate_sorters_entry_list():
            s = "\n"
            for m in range(1, self.max_bitonic_sort_vectors + 1):
                s += f"\t\t\t\tcase {m}: sort_{m:02d}v_alt(ptr, remainder); return; \n"
            return s

        t = self.type
        s = f"""                
                                               
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static void Sort({t}* ptr, int length)
        {{                    
            uint fullvlength = (uint)length / (uint)V.Count;
            var remainder = (int)(length - fullvlength * V.Count);
            var v = fullvlength + ((remainder > 0) ? 1 : 0);
            switch (v)
            {{
                {generate_sorters_entry_list()}              
            }}
        }}"""
        print(s, file=f_header)


    def generate_main(self, f_header):
        def generate_bitonic_length_by_type():
            s = ""
            for key, type in self.bitonic_type_map.items():
                s += f"""
            if (typeof(T) == typeof({key}))
                return {self.bitonic_size_map[key]} * {self.max_bitonic_sort_vectors};"""
            return s

        t = self.type
        s = f"""                
        // * We might read the last 4 bytes into a 128-bit vector for 64-bit element masking
        // * We might read the last 8 bytes into a 128-bit vector for 32-bit element masking
        // This mostly applies to debug mode, since without optimizations, most compilers
        // actually execute the instruction stream _mm256_cvtepi8_epiNN + _mm_loadu_si128 as they are given.
        // In contract, release/optimizing compilers, turn that very specific instruction pair to
        // a more reasonable: vpmovsxbq ymm0, dword [rax*4 + mask_table_4], eliminating the 128-bit
        // load completely and effectively reading 4/8 (depending if the instruction is vpmovsxb[q,d]

        public static ReadOnlySpan<byte> mask_table_4 => new byte[]{{
            0xFF, 0xFF, 0xFF, 0xFF, // 0b0000 (0)
            0xFF, 0x00, 0x00, 0x00, // 0b0001 (1)
            0xFF, 0xFF, 0x00, 0x00, // 0b0011 (3)
            0xFF, 0xFF, 0xFF, 0x00, // 0b0111 (7)
            0xCC, 0xCC, 0xCC, 0xCC, // Ensuring we cannot overrun the buffer.
            0xCC, 0xCC, 0xCC, 0xCC, // Ensuring we cannot overrun the buffer.
            0xCC, 0xCC, 0xCC, 0xCC, // Ensuring we cannot overrun the buffer.
        }};

        public static ReadOnlySpan<byte> mask_table_8 => new byte[]{{
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 0b00000000 (  0)
            0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000001 (  1)
            0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000011 (  3)
            0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, // 0b00000111 (  7)
            0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, // 0b00001111 ( 15)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, // 0b00011111 ( 31)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, // 0b00111111 ( 63)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, // 0b01111111 (127)
            0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, // Ensuring we cannot overrun the buffer.
        }};
        
        public static int MaxBitonicLength<T>() where T : unmanaged
        {{
            { generate_bitonic_length_by_type() }
            
            throw new NotSupportedException($"The type {{typeof(T).Name}} is not supported");
        }} 
                
        """
        print(s, file=f_header)
