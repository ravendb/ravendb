using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Security;
using System.Security.Permissions;
using System.Text;
using Mono.Reflection;

namespace Raven.Database.Linq
{
	public static class CodeVerifier
	{
		public static bool Active { get; set; }

		static CodeVerifier()
		{
			var limitIndexesCapabilities =
				System.Configuration.ConfigurationManager.AppSettings["Raven/LimitIndexesCapabilities"];

			if (string.IsNullOrEmpty(limitIndexesCapabilities))
				return;

			Active = bool.Parse(limitIndexesCapabilities);
		}

		private static readonly string[] forbiddenNamespaces = new[]
		{
			typeof(System.IO.File).Namespace,
			typeof(System.Threading.Thread).Namespace,
			typeof(Microsoft.Win32.Registry).Namespace,
			typeof(System.Net.WebRequest).Namespace,
			typeof(System.Data.IDbConnection).Namespace,
			typeof(System.ServiceModel.ChannelFactory).Namespace,
			typeof(System.Security.IPermission).Namespace,
			typeof(System.Transactions.Transaction).Namespace,
			typeof(System.Reflection.Assembly).Namespace,
			typeof(System.Configuration.ConfigurationManager).Namespace,
			typeof(Microsoft.Isam.Esent.EsentException).Namespace,
		};
		
		private static readonly Type[] forbiddenTypes = new[]
		{
			typeof (Environment),
			typeof(AppDomain),
			typeof(AppDomainManager),
			typeof(CodeVerifier),
		};

		public static void AssertNoSecurityCriticalCalls(Assembly asm)
		{
			if (Active == false)
				return;

			foreach (var type in asm.GetTypes())
			{
				foreach (var methodInfo in type.GetMethods(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
				{
					if (methodInfo.DeclaringType != type)
						continue;
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
						var msg = PrepareSecurityMessage(memberInfo);
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
			var attributes = GetAttributesForMethodAndType(memberInfo, typeof(SecurityCriticalAttribute))
				.Concat(GetAttributesForMethodAndType(memberInfo, typeof(HostProtectionAttribute)))
				.Where(HasSecurityIssue).ToArray();

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