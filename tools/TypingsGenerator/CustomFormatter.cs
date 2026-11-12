using System;
using TypeScripter.TypeScript;

namespace TypingsGenerator
{
    public class CustomFormatter : TsFormatter
    {
        private const string ModuleDeclaration = "declare module ";
        private const string NamespaceDeclaration = "declare namespace ";

        public override string Format(TsModule module)
        {
            string formatted = base.Format(module);
            int declarationIndex = formatted.IndexOf(ModuleDeclaration, StringComparison.Ordinal);
            if (declarationIndex < 0)
                return formatted;

            return formatted
                .Remove(declarationIndex, ModuleDeclaration.Length)
                .Insert(declarationIndex, NamespaceDeclaration);
        }
    }
}
