using System;

namespace Raven.Imports.Newtonsoft.Json
{
  /// <summary>
  /// Instructs the <see cref="JsonSerializer"/> to populate properties with no matching class member onto the specified collection.
  /// </summary>
  [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
  public class JsonExtensionDataAttribute : Attribute
  {
  }
}