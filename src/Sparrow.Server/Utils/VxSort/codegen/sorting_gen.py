#!/usr/bin/env python3
#
# This is a tool to generate the vectorized sorter code that is used for big arrays.
#
# usage: sorting_gen.py [-h] [--output-dir OUTPUT_DIR]
#
#
import argparse
import os

from configuration import Configuration
from sorting_isa import SortingISA
from sorting_avx2 import AVX2SortingISA

from enum import Enum
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime

SortingISA.register(AVX2SortingISA)

class VectorISA(Enum):
    AVX2 = 'AVX2'
    # AVX512 = 'AVX512'
    # SVE = 'SVE'

    def __str__(self):
        return self.value


def get_generator_supported_types(vector_isa):
    if isinstance(vector_isa, str):
        vector_isa = VectorISA[vector_isa]
    if vector_isa == VectorISA.AVX2:
        return AVX2SortingISA.supported_types()
    # elif vector_isa == VectorISA.AVX512:
    #     return AVX512SortingISA.supported_types()
    else:
        raise Exception(f"Non-supported vector machine-type: {vector_isa}")


def get_generator(vector_isa, type, configuration):
    if isinstance(vector_isa, str):
        vector_isa = VectorISA[vector_isa]
    if vector_isa == VectorISA.AVX2:
        return AVX2SortingISA(type, configuration)
    # elif vector_isa == VectorISA.AVX512:
    #     return AVX512BitonicISA(type)
    else:
        raise Exception(f"Non-supported vector machine-type: {vector_isa}")


def generate_per_type(f_header, type, vector_isa, configuration):
    g = get_generator(vector_isa, type, configuration)
    g.generate_prologue(f_header)
    g.generate_entry_points(f_header)
    g.generate_epilogue(f_header)


def generate_base_types(f_header, vector_isa, configuration):
    g = get_generator(vector_isa, type, configuration)
    g.generate_prologue(f_header)
    g.generate_master_entry_point(f_header)
    g.generate_epilogue(f_header)


def generate_sorting_all_types():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vector-isa",
                        nargs='+',
                        default='all',
                        help='list of vector ISA to generate',
                        choices=list(VectorISA).append("all"))
    parser.add_argument("--output-dir", type=str, help="output directory")

    opts = parser.parse_args()

    if 'all' in opts.vector_isa:
        opts.vector_isa = list(VectorISA)

    config = Configuration()

    for isa in opts.vector_isa:
        filename = f"VectorizedSort.{isa}.generated"
        print(f"Generating {filename}.{{cs}}")
        h_filename = os.path.join(opts.output_dir, filename + ".cs")
        with open(h_filename, "w") as f_header:
            generate_base_types(f_header, isa, config)

        for t in get_generator_supported_types(isa):
            filename = f"VectorizedSort.{isa}.{t}.generated"
            print(f"Generating {filename}.{{cs}}")
            h_filename = os.path.join(opts.output_dir, filename + ".cs")
            with open(h_filename, "w") as f_header:
                generate_per_type(f_header, t, isa, config)


if __name__ == '__main__':
    generate_sorting_all_types()
