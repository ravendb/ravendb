using System;

namespace Raven.Abstractions.Logging
{
	public interface ILogManager
	{
		ILog GetLogger(string name);

		IDisposable OpenNestedConext(string message);

		IDisposable OpenMappedContext(string key, string value);
	}
}