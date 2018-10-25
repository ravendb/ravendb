using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using Lucene.Net.Search;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public static class SorterCompiler
    {
        public static Func<string, int, int, bool, FieldComparator> Compile(string name, string sorterCode)
        {
            var compilationUnit = SyntaxFactory.ParseCompilationUnit(sorterCode);

            SyntaxNode formattedCompilationUnit;
            using (var workspace = new AdhocWorkspace())
            {
                formattedCompilationUnit = Formatter.Format(compilationUnit, workspace);
            }

            string sourceFile = null;

            if (IndexCompiler.EnableDebugging)
            {
                sourceFile = Path.Combine(Path.GetTempPath(), name + ".cs");
                File.WriteAllText(sourceFile, formattedCompilationUnit.ToFullString(), Encoding.UTF8);
            }

            var syntaxTree = IndexCompiler.EnableDebugging
                ? SyntaxFactory.ParseSyntaxTree(File.ReadAllText(sourceFile), path: sourceFile, encoding: Encoding.UTF8)
                : SyntaxFactory.ParseSyntaxTree(formattedCompilationUnit.ToFullString());

            var compilation = CSharpCompilation.Create(
                assemblyName: name + ".sorter.dll",
                syntaxTrees: new[] { syntaxTree },
                references: IndexCompiler.References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(IndexCompiler.EnableDebugging ? OptimizationLevel.Debug : OptimizationLevel.Release)
            );

            var code = formattedCompilationUnit.SyntaxTree.ToString();

            var asm = new MemoryStream();
            var pdb = IndexCompiler.EnableDebugging ? new MemoryStream() : null;

            var result = compilation.Emit(asm, pdb, options: new EmitOptions(debugInformationFormat: DebugInformationFormat.PortablePdb));

            if (result.Success == false)
            {
                IEnumerable<Diagnostic> failures = result.Diagnostics
                    .Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);

                var sb = new StringBuilder();
                sb.AppendLine($"Failed to compile sorter '{name}'");
                sb.AppendLine();
                sb.AppendLine(code);
                sb.AppendLine();

                foreach (var diagnostic in failures)
                    sb.AppendLine(diagnostic.ToString());


                throw new IndexCompilationException(sb.ToString());

            }

            asm.Position = 0;

            Assembly assembly;

            if (IndexCompiler.EnableDebugging)
            {
                pdb.Position = 0;
                assembly = AssemblyLoadContext.Default.LoadFromStream(asm, pdb);
            }
            else
            {
                assembly = AssemblyLoadContext.Default.LoadFromStream(asm);
            }

            var type = assembly.GetType(name);
            if (type == null)
            {
                foreach (var exportedType in assembly.GetExportedTypes())
                {
                    if (exportedType.Name != name) 
                        continue;

                    type = exportedType;
                    break;
                }
            }

            if (type == null)
                throw new InvalidOperationException();

            return (fieldName, numHits, sortPos, reversed) =>
            {
                var instance = Activator.CreateInstance(type, fieldName, numHits, sortPos, reversed) as FieldComparator;
                if (instance == null)
                    throw new InvalidOperationException();

                return instance;
            };
        }
    }
}
