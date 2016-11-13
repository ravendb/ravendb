using System;

using Raven.Abstractions.Extensions;

namespace Raven.NewClient.Client.Exceptions
{
    public class IndexCompilationException : Exception
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
                                          description.AppendFormat(
                                              ", IndexDefinitionProperty='{0}', ProblematicText='{1}'",
                                              IndexDefinitionProperty,
                                              ProblematicText));
        }
    }
}
