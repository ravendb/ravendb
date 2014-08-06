using System;
using System.Reflection;

using Raven.Abstractions.Extensions;

namespace Raven.Client.UniqueConstraints
{
	public class ReflectedConstraintInfo : ConstraintInfo
	{
		public ReflectedConstraintInfo(MemberInfo member, UniqueConstraintAttribute attr)
		{
			if (member == null) { throw new ArgumentNullException("member"); }

			this.Member = member;
			this.Configuration.Name = member.Name;

			if (attr != null)
			{
				this.Configuration.CaseInsensitive = attr.CaseInsensitive;
			}
		}

		public MemberInfo Member { get; private set; }

		public override object GetValue(object entity)
		{
			object value = Member.GetValue(entity);

			if (value == null) { return null; }

			return value;
		}
	}
}
