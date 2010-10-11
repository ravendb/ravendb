using System;
using System.Collections.Generic;
using log4net;

namespace Raven.Database.Extensions
{
	public static class EnumerableExtensions
	{
		public static void Apply<T>(this IEnumerable<T> self, Action<T> action)
		{
			foreach (var item in self)
			{
				action(item);
			}
		}

        public static void ApplyAndIgnoreAllErrors<T>(this IEnumerable<T> self, Action<Exception> errorAction, Action<T> action)
        {
            foreach (var item in self)
            {
                try
                {
                    action(item);
                }
                catch (Exception e)
                {
                    errorAction(e);
                }
            }
        }
	}
}
