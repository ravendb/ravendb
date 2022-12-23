=== VxSort (Bitonic Sort + SIMD) ===

This is a reimplementation of VxSort originally developed by @damageboy in C# (https://github.com/damageboy/VxSort) and then implemented in C++ (https://github.com/dotnet/runtime/tree/57bfe474518ab5b7cfe6bf7424a79ce3af9d6657/src/coreclr/gc/vxsort) and currently in use by the CoreCLR garbage collector for sorting the objects lists.

The current implementation supports the ability to pack originally found on the C++ version of VxSort and it also supports to unfold the complete bitonic sorting intrinsics code to avoid the JIT not inlining certain methods and miss the optimizations. 

For this implementation having requested permission I (@redknightlois) repurposed the original code generator to build an updated version that resembles the more advanced C++ implementation. Under request of the original developer, this code generator is provided as-is and only attribution to the developers is requested:

Dan Shechter (@damageboy)
Federico Lois (@redknightlois)
The RavenDB Team (@ravendb)

For more information on the basic behavior behind this algorithm, please see https://bits.houmus.org/2020-01-28/this-goes-to-eleven-pt1 and https://www.youtube.com/watch?v=ug_UC4lxMr8 for details.
