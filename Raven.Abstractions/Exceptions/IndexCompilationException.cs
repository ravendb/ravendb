using System;
using System.Runtime.Serialization;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Exceptions
{
    public class IndexCompilationException : Exception
    {
        public IndexCompilationException(string message) : base(message)
        {
        }

        public IndexCompilationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public override string ToString()
        {
            return this.ExceptionToString(description =>
                                          description.AppendFormat(
                                              ", IndexDefinitionProperty='{0}', ProblematicText='{1}'",
                                              IndexDefinitionProperty,
                                              ProblematicText));
        }

        public string IndexDefinitionProperty { get; set; }

        public string ProblematicText { get; set; }
    }
}
