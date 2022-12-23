

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
                
        """
        print(s, file=f)

    def generate_public_api(self, f):
        pass

    def generate_epilogue(self, f):
        s = F"""
    }}
}}
"""
        print(s, file=f)
