namespace Raven.Client.UniqueConstraints
{
	using System;

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public class UniqueConstraintAttribute : Attribute
	{
        public bool CaseInsensitive { get; set; }
	}
}