using System;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public class IndexCompilationException : CompilationException
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
            return this.ExceptionToString(description => description.AppendFormat(", IndexDefinitionProperty='{0}', ProblematicText='{1}'", IndexDefinitionProperty, ProblematicText));
        }
    }
}
