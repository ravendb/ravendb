namespace Raven.Studio.Framework
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Reflection;

	public static class Enumeration
	{
		public static IEnumerable<T> All<T>() where T : class, IComparable
		{
			return
				from field in typeof (T).GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
				let val = field.GetValue(null) as T
				where val != null
				select val;
		}
	}

	//TODO: the syntax here could be nicer
	public abstract class Enumeration<T> : IComparable where T : IComparable
	{
		readonly string displayName;
		readonly T value;

		protected Enumeration()
		{
		}

		protected Enumeration(T value, string displayName)
		{
			this.value = value;
			this.displayName = displayName;
		}

		public T Value
		{
			get { return value; }
		}

		public string DisplayName
		{
			get { return displayName; }
		}

		public int CompareTo(object other)
		{
			return Value.CompareTo(((Enumeration<T>) other).Value);
		}

		public override string ToString()
		{
			return DisplayName;
		}

		public override bool Equals(object obj)
		{
			var otherValue = obj as Enumeration<T>;

			if (otherValue == null) return false;

			var typeMatches = GetType().Equals(obj.GetType());
			var valueMatches = value.Equals(otherValue.Value);

			return typeMatches && valueMatches;
		}

		public override int GetHashCode()
		{
			return value.GetHashCode();
		}

		public static TK FromValue<TK>(T value, TK defaultValue)
			where TK : Enumeration<T>
		{
			return Parse(item => item.Value.Equals(value), defaultValue);
		}

		public static TK FromDisplayName<TK>(string displayName, TK defaultValue)
			where TK : Enumeration<T>
		{
			return Parse(item => item.DisplayName == displayName, defaultValue);
		}

		public static bool ContainsValue<TK>(T value)
			where TK : Enumeration<T>
		{
			return Enumeration.All<TK>().Any(x => x.Value.Equals(value));
		}

		static TK Parse<TK>(Func<TK, bool> predicate, TK defaultValue)
			where TK : Enumeration<T>
		{
			return Enumeration.All<TK>().FirstOrDefault(predicate) ?? defaultValue;
		}

		public static implicit operator T(Enumeration<T> enumeration)
		{
			return enumeration.Value;
		}
	}
}