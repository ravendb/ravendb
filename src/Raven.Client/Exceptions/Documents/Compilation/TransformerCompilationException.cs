using System;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public class TransformerCompilationException : CompilationException
    {
        public TransformerCompilationException()
        {
        }

        public TransformerCompilationException(string message)
            : base(message)
        {
        }

        public TransformerCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Indicates which property caused error (TransformResults).
        /// </summary>
        public string TransformerDefinitionProperty;

        /// <summary>
        /// Value of a problematic property.
        /// </summary>
        public string ProblematicText;

        public override string ToString()
        {
            return this.ExceptionToString(description => description.AppendFormat(", TransformerDefinitionProperty='{0}', ProblematicText='{1}'", TransformerDefinitionProperty, ProblematicText));
        }
    }
}
