using System;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;

namespace Raven.Tests.Triggers.Bugs
{
	public static class AuditContext
	{
		[ThreadStatic]
		private static bool _currentlyInContext;

		public static bool IsInAuditContext
		{
			get
			{
				return _currentlyInContext;
			}
		}

		public static IDisposable Enter()
		{
			var old = _currentlyInContext;
			_currentlyInContext = true;
			return new DisposableAction(() => _currentlyInContext = old);
		}
	}
}