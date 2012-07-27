using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Reflection.Emit;
using System.Security;
using System.Security.Permissions;
using System.Text;
using System.Threading;
using Microsoft.Win32;
using Mono.Reflection;

namespace Raven.Database.Linq
{
	public static class CodeVerifier
	{
		private static readonly ConcurrentDictionary<MemberInfo, string> cache = new ConcurrentDictionary<MemberInfo, string>();

		private static readonly string[] forbiddenNamespaces = new[]
		{
			typeof(File).Namespace,
			typeof(Thread).Namespace,
			typeof(Registry).Namespace,
			typeof(WebRequest).Namespace
		};
		
		private static readonly Type[] forbiddenTypes = new[]
		{
			typeof (Environment)
		};

		public static void AssertNoSecurityCriticalCalls(Assembly asm)
		{
			foreach (var type in asm.GetTypes())
			{
				foreach (var methodInfo in type.GetMethods(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
				{
					foreach (var instruction in methodInfo.GetInstructions())
					{
						if (instruction.OpCode != OpCodes.Call && instruction.OpCode != OpCodes.Call &&
						    instruction.OpCode != OpCodes.Callvirt && instruction.OpCode != OpCodes.Newobj)
							continue;

						var memberInfo = instruction.Operand as MemberInfo;
						if (memberInfo == null)
						{
							continue;
						}

						string value;
						if (cache.TryGetValue(memberInfo, out value))
						{
							if (value == null)
								continue;
							throw new SecurityException(value);
						}

						var msg = PrepareSecurityMessage(memberInfo);

						cache.TryAdd(methodInfo, msg);

						if (msg == null)
							continue;

						throw new SecurityException(msg);
					}
				}
			}
		}

		private static IEnumerable<object> GetAttributesForMethodAndType(MemberInfo memberInfo, Type type)
		{
			var declaringType = memberInfo.DeclaringType;
			if (declaringType == null)
				return memberInfo.GetCustomAttributes(type, true);
			return memberInfo.GetCustomAttributes(type, true)
				.Concat(declaringType.GetCustomAttributes(type, true));
		}

		private static string PrepareSecurityMessage(MemberInfo memberInfo)
		{
			var attributes = Enumerable.ToArray(GetAttributesForMethodAndType(memberInfo, typeof(SecurityCriticalAttribute))
			                                    	.Concat(GetAttributesForMethodAndType(memberInfo, typeof(HostProtectionAttribute)))
			                                    	.Where(HasSecurityIssue));

			var forbiddenNamespace =
				forbiddenNamespaces.FirstOrDefault(
					x =>
					memberInfo.DeclaringType != null && memberInfo.DeclaringType.Namespace != null &&
					memberInfo.DeclaringType.Namespace.StartsWith(x));

			var forbiddenType = forbiddenTypes.FirstOrDefault(x => x == memberInfo.DeclaringType);



			if (attributes.Length == 0 && forbiddenNamespace == null && forbiddenType == null)
			{
				return null;

			}
			var sb = new StringBuilder();
			sb.Append("Cannot use an index which calls method '")
				.Append(memberInfo.DeclaringType == null ? "" : memberInfo.DeclaringType.FullName)
				.Append(".")
				.Append(memberInfo.Name)
				.AppendLine(" because it or its declaring type has been marked as not safe for indexing.");

			if (forbiddenNamespace != null && memberInfo.DeclaringType != null)
				sb.Append("\tCannot use methods on namespace: ").Append(memberInfo.DeclaringType.Namespace).AppendLine();

			if (forbiddenType != null)
				sb.Append("\tCannot use methods from type: ").Append(forbiddenType.FullName).AppendLine();

			foreach (var attribute in attributes)
			{
				if (attribute is SecurityCriticalAttribute)
					sb.AppendLine("\tMarked with [SecurityCritical] attribute");

				if (attribute is SecuritySafeCriticalAttribute)
					sb.AppendLine("\tMarked with [SecuritySafeCritical] attribute");

				var hostProtectionAttribute = attribute as HostProtectionAttribute;
				if (hostProtectionAttribute == null)
					continue;

				sb.Append("\t[HostProtection(Action = ")
					.Append(hostProtectionAttribute.Action)
					.Append(", ExternalProcessMgmt = ")
					.Append(hostProtectionAttribute.ExternalProcessMgmt)
					.Append(", ExternalThreading = ")
					.Append(hostProtectionAttribute.ExternalThreading)
					.Append(", Resources = ")
					.Append(hostProtectionAttribute.Resources)
					.Append(", SecurityInfrastructure = ")
					.Append(hostProtectionAttribute.SecurityInfrastructure)
					.Append(", SelfAffectingProcessMgmt = ")
					.Append(hostProtectionAttribute.SelfAffectingProcessMgmt)
					.Append(", SelfAffectingThreading = ")
					.Append(hostProtectionAttribute.SelfAffectingThreading)
					.Append(", SharedState = ")
					.Append(hostProtectionAttribute.SharedState)
					.Append(", Synchronization = ")
					.Append(hostProtectionAttribute.Synchronization)
					.Append(", MayLeakOnAbort = ")
					.Append(hostProtectionAttribute.MayLeakOnAbort)
					.Append(", Unrestricted = ")
					.Append(hostProtectionAttribute.Unrestricted)
					.Append(", UI =")
					.Append(hostProtectionAttribute.UI)
					.AppendLine("]");
			}

			return sb.ToString();
		}

		private static bool HasSecurityIssue(object arg)
		{
			var hostProtectionAttribute = arg as HostProtectionAttribute;
			if (hostProtectionAttribute == null)
				return true;

			if (hostProtectionAttribute.ExternalProcessMgmt ||
			    hostProtectionAttribute.ExternalThreading ||
			    (hostProtectionAttribute.Resources != HostProtectionResource.None && hostProtectionAttribute.Resources != HostProtectionResource.MayLeakOnAbort) ||
			    hostProtectionAttribute.SecurityInfrastructure ||
			    hostProtectionAttribute.SelfAffectingProcessMgmt ||
			    hostProtectionAttribute.SelfAffectingThreading ||
			    hostProtectionAttribute.SharedState ||
			    hostProtectionAttribute.Synchronization ||
			    hostProtectionAttribute.UI ||
			    hostProtectionAttribute.Unrestricted)
				return true;

			// we can live with this one
			if (hostProtectionAttribute.MayLeakOnAbort)
				return false;

			//maybe something else happened?
			return true;
		}
	}
}