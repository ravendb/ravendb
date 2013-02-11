using System;
using System.Linq;
using System.Reflection;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public static class TypeExtensions
	{
		public static bool IsInstanceOfType(this Type type, object o)
		{
			return o != null && type.IsAssignableFrom(o.GetType().GetTypeInfo());
		}

		internal static bool ImplementInterface(this Type type, Type ifaceType)
		{
			while (type != null)
			{
				Type[] interfaces = type.GetTypeInfo().ImplementedInterfaces.ToArray(); //  .GetInterfaces();
				if (interfaces != null)
				{
					for (int i = 0; i < interfaces.Length; i++)
					{
						if (interfaces[i] == ifaceType || (interfaces[i] != null && interfaces[i].ImplementInterface(ifaceType)))
						{
							return true;
						}
					}
				}
				type = type.GetTypeInfo().BaseType;
				// type = type.BaseType;
			}
			return false;
		}
	}
}