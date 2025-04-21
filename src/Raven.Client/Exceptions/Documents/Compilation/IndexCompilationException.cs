using System;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public sealed class IndexCompilationException : CompilationException
    {
        public IndexCompilationException()
        {
        }

        public IndexCompilationException(string message)
            : base(message)
        {
        }

        public IndexCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Indicates which property caused error (Maps, Reduce).
        /// </summary>
        public string IndexDefinitionProperty;

        /// <summary>
        /// Value of a problematic property.
        /// </summary>
        public string ProblematicText;

        public override string ToString()
        {
            return this.ExceptionToString(description =>
                description.AppendFormat(", IndexDefinitionProperty='{0}', ProblematicText='{1}'", IndexDefinitionProperty, ProblematicText));
        }
        
#if !NETSTANDARD2_0
        [DoesNotReturn]
#endif
        internal static void ThrowFor(string indexName, string message)
        {
            throw new IndexCompilationException($"Failed to compile index '{indexName}': {message}");
        }

#if !NETSTANDARD2_0
        [DoesNotReturn]
#endif
        internal static void ThrowFor(string indexName, Exception inner)
        {
            throw new IndexCompilationException($"Failed to compile index '{indexName}'.", inner);
        }

#if !NETSTANDARD2_0
        [DoesNotReturn]
#endif
        internal static void ThrowFor(string indexName, string message, Exception inner)
        {
            throw new IndexCompilationException($"Failed to compile index '{indexName}': {message}", inner);
        }

#if !NETSTANDARD2_0
        [DoesNotReturn]
#endif
        internal static void ThrowFor(string indexName, string message, Exception inner, string indexDefinitionProperty, string problematicText)
        {
            throw new IndexCompilationException($"Failed to compile index '{indexName}': {message}", inner)
            {
                IndexDefinitionProperty = indexDefinitionProperty, ProblematicText = problematicText
            };
        }
    }
}
