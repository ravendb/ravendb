/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Support;
using TokenStream = Lucene.Net.Analysis.TokenStream;

namespace Lucene.Net.Util
{
	
	/// <summary> An AttributeSource contains a list of different <see cref="Attribute" />s,
	/// and methods to add and get them. There can only be a single instance
	/// of an attribute in the same AttributeSource instance. This is ensured
	/// by passing in the actual type of the Attribute (Class&lt;Attribute&gt;) to 
	/// the <see cref="AddAttribute{T}()" />, which then checks if an instance of
	/// that type is already present. If yes, it returns the instance, otherwise
	/// it creates a new instance and returns it.
	/// </summary>
	public class AttributeSource
	{
		/// <summary> An AttributeFactory creates instances of <see cref="Attribute" />s.</summary>
		public abstract class AttributeFactory
		{
			/// <summary> returns an <see cref="Attribute" /> for the supplied <see cref="IAttribute" /> interface class.</summary>
			public abstract Attribute CreateAttributeInstance<T>() where T : IAttribute;
			
			/// <summary> This is the default factory that creates <see cref="Attribute" />s using the
			/// class name of the supplied <see cref="IAttribute" /> interface class by appending <c>Impl</c> to it.
			/// </summary>
			public static readonly AttributeFactory DEFAULT_ATTRIBUTE_FACTORY = new DefaultAttributeFactory();
			
			private sealed class DefaultAttributeFactory:AttributeFactory
			{
                // This should be WeakDictionary<T, WeakReference<TImpl>> where typeof(T) is Attribute and TImpl is typeof(AttributeImpl)
			    private static Dictionary<Type, Func<object>> attClassImplMap =
			        new Dictionary<Type, Func<object>>();
                			
				public override Attribute CreateAttributeInstance<TAttImpl>()
				{
					try
					{
						var ctor = GetClassForInterface<TAttImpl>();
						return (Attribute)ctor();
					}
					catch (UnauthorizedAccessException)
                    {
                        throw new ArgumentException("Could not instantiate implementing class for " + typeof(TAttImpl).FullName);
					}
                    //catch (System.Exception e)
                    //{
                    //    throw new System.ArgumentException("Could not instantiate implementing class for " + typeof(TAttImpl).FullName);
                    //}
				}

                private static Func<object> GetClassForInterface<T>() where T : IAttribute
				{
                    var attClass = typeof(T);

					if (attClassImplMap.TryGetValue(attClass, out var ctor) == false)
                    {
						try
						{
							string name = attClass.FullName.Replace(attClass.Name, attClass.Name.Substring(1)) + ", " +
										  attClass.Assembly.FullName;
							var clazz = Type.GetType(name, true);

							var ctorz = clazz
								.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
								.FirstOrDefault(x => x.GetParameters().Length == 0);
							var instance = ctorz != null
								? Expression.New(ctorz)
								: Expression.New(clazz);

							var lambda = Expression.Lambda<Func<object>>(instance);

							ctor = lambda.Compile();

							attClassImplMap = new Dictionary<Type, Func<object>>(attClassImplMap) { [attClass] = ctor };
						}
						catch (TypeLoadException) // was System.Exception
						{
							throw new ArgumentException("Could not find implementing class for " + attClass.FullName);
						}
					}

					return ctor;
                }
			}
		}
		
		// These two maps must always be in sync!!!
		// So they are private, final and read-only from the outside (read-only iterators)
        private readonly Dictionary<Type, AttributeImplItem> attributes;
	    private readonly Dictionary<Type, AttributeImplItem> attributeImpls;

        private readonly State[] currentState = null;
	    private readonly AttributeFactory factory;
		
		/// <summary> An AttributeSource using the default attribute factory <see cref="AttributeSource.AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY" />.</summary>
		public AttributeSource():this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY)
		{
		}
		
		/// <summary> An AttributeSource that uses the same attributes as the supplied one.</summary>
		public AttributeSource(AttributeSource input)
		{
			if (input == null)
			{
				throw new System.ArgumentException("input AttributeSource must not be null");
			}
			this.attributes = input.attributes;
			this.attributeImpls = input.attributeImpls;
		    this.currentState = input.currentState;
			this.factory = input.factory;
		}
		
		/// <summary> An AttributeSource using the supplied <see cref="AttributeFactory" /> for creating new <see cref="IAttribute" /> instances.</summary>
		public AttributeSource(AttributeFactory factory)
		{
		    this.attributes = new Dictionary<Type, AttributeImplItem>(IdentityStructComparer<Type>.Default); //att => att.Key);
            this.attributeImpls = new Dictionary<Type, AttributeImplItem>(IdentityStructComparer<Type>.Default); // att => att.Key);
            this.currentState = new State[1];
            this.factory = factory;
		}

	    /// <summary>Returns the used AttributeFactory.</summary>
	    public virtual AttributeFactory Factory
	    {
	        get { return factory; }
	    }

	    /// <summary>Returns a new iterator that iterates the attribute classes
		/// in the same order they were added in.
		/// Signature for Java 1.5: <c>public Iterator&lt;Class&lt;? extends Attribute&gt;&gt; getAttributeClassesIterator()</c>
		///
		/// Note that this return value is different from Java in that it enumerates over the values
		/// and not the keys
		/// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual IEnumerable<Type> GetAttributeTypesIterator()
		{
            return this.attributes.Select(item => item.Key);
		}

        /// <summary>Returns a new iterator that iterates all unique Attribute implementations.
        /// This iterator may contain less entries that <see cref="GetAttributeTypesIterator" />,
        /// if one instance implements more than one Attribute interface.
        /// Signature for Java 1.5: <c>public Iterator&lt;AttributeImpl&gt; getAttributeImplsIterator()</c>
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual IEnumerable<Attribute> GetAttributeImplsIterator()
        {
            var initState = GetCurrentState();
            while (initState != null)
            {
                var att = initState.attribute;
                initState = initState.next;
                yield return att;
            }
        }

	    /// <summary>a cache that stores all interfaces for known implementation classes for performance (slow reflection) </summary>
	    private static readonly ConditionalWeakTable<Type, LinkedList<WeakReference>> knownImplClasses = new ConditionalWeakTable<Type, LinkedList<WeakReference>>();

        /// <summary>
        /// <b>Expert:</b> Adds a custom AttributeImpl instance with one or more Attribute interfaces.
        /// <p><font color="red"><b>Please note:</b> It is not guaranteed, that <c>att</c> is added to
        /// the <c>AttributeSource</c>, because the provided attributes may already exist.
        /// You should always retrieve the wanted attributes using <see cref="GetAttribute{T}"/> after adding
        /// with this method and cast to your class.
        /// The recommended way to use custom implementations is using an <see cref="AttributeFactory"/>
        /// </font></p>
        /// </summary>
		public void AddAttributeImpl(Attribute att)
		{
			Type clazz = att.GetType();
		    
			if (attributeImpls.TryGetValue(clazz, out var impl))
				return;

            if (knownImplClasses.TryGetValue(clazz, out var foundInterfaces) == false)
            {
                foundInterfaces = new LinkedList<WeakReference>();
                Type actClazz = clazz;
                do 
                {
                    Type[] interfaces = actClazz.GetInterfaces();
                    for (int i = 0; i < interfaces.Length; i++)
                    {
                        Type curInterface = interfaces[i];
                        if (curInterface != typeof(IAttribute) && typeof(IAttribute).IsAssignableFrom(curInterface))
                        {
                            foundInterfaces.AddLast(new WeakReference(curInterface));
                        }
                    }
                    actClazz = actClazz.BaseType;
                }
                while (actClazz != null);

                knownImplClasses.AddOrUpdate(clazz, foundInterfaces);
            }
			
			// add all interfaces of this AttributeImpl to the maps
			foreach(var curInterfaceRef in foundInterfaces)
			{
				System.Type curInterface = (System.Type) curInterfaceRef.Target;
			    System.Diagnostics.Debug.Assert(curInterface != null,
			                                    "We have a strong reference on the class holding the interfaces, so they should never get evicted");
				
                // Attribute is a superclass of this interface
                if (!attributes.TryGetValue(curInterface, out var _))
				{
					// invalidate state to force recomputation in captureState()
					this.currentState[0] = null;
                    attributes[curInterface] = new AttributeImplItem(curInterface, att);
                    if (!attributeImpls.TryGetValue(clazz, out _))
                    {
                        attributeImpls[clazz] = new AttributeImplItem(clazz, att);
                    }
				}
			}
		}

        /// <summary> The caller must pass in a Class&lt;? extends Attribute&gt; value.
        /// This method first checks if an instance of that class is 
        /// already in this AttributeSource and returns it. Otherwise a
        /// new instance is created, added to this AttributeSource and returned. 
        /// </summary>
        // NOTE: Java has Class<T>, .NET has no Type<T>, this is not a perfect port
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T AddAttribute<T>() where T : IAttribute
		{
		    var attClass = typeof (T);

		    if (attributes.TryGetValue(attClass, out var value))
		        return (T) (IAttribute) value.Value;

		    return AddAttributeUnlikely<T>(attClass);
		}

	    private T AddAttributeUnlikely<T>(Type attClass) where T : IAttribute
	    {
	        if (!(attClass.IsInterface && typeof(IAttribute).IsAssignableFrom(attClass)))
	        {
	            throw new ArgumentException(
	                "AddAttribute() only accepts an interface that extends Attribute, but " +
	                attClass.FullName + " does not fulfil this contract."
	            );
	        }

	        AddAttributeImpl(this.factory.CreateAttributeInstance<T>());
	        return (T) (IAttribute) attributes[attClass].Value;
	    }

	    /// <summary>Returns true, iff this AttributeSource has any attributes </summary>
	    public bool HasAttributes
	    {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
	        get { return this.attributes.Count != 0; }
	    }

	    /// <summary> The caller must pass in a Class&lt;? extends Attribute&gt; value. 
		/// Returns true, iff this AttributeSource contains the passed-in Attribute.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool HasAttribute<T>() where T : IAttribute
		{            
			return this.attributes.TryGetValue(typeof(T), out var _);
		}

        /// <summary>
        /// The caller must pass in a Class&lt;? extends Attribute&gt; value. 
        /// Returns the instance of the passed in Attribute contained in this AttributeSource
        /// </summary>
        /// <throws>
        /// IllegalArgumentException if this AttributeSource does not contain the Attribute. 
        /// It is recommended to always use <see cref="AddAttribute{T}" /> even in consumers
        /// of TokenStreams, because you cannot know if a specific TokenStream really uses
        /// a specific Attribute. <see cref="AddAttribute{T}" /> will automatically make the attribute
        /// available. If you want to only use the attribute, if it is available (to optimize
        /// consuming), use <see cref="HasAttribute" />.
        /// </throws>
        // NOTE: Java has Class<T>, .NET has no Type<T>, this is not a perfect port
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetAttribute<T>() where T : IAttribute
        {
            var attClass = typeof (T);
            if (this.attributes.ContainsKey(attClass))
                return (T) (IAttribute) this.attributes[attClass].Value;

            return ThrowArgumentException<T>(attClass);
        }

	    private static T ThrowArgumentException<T>(Type attClass) where T : IAttribute
	    {
	        throw new ArgumentException("This AttributeSource does not have the attribute '" + attClass.FullName + "'.");
	    }

	    /// <summary> This class holds the state of an AttributeSource.</summary>
		/// <seealso cref="CaptureState">
		/// </seealso>
		/// <seealso cref="RestoreState">
		/// </seealso>
		public sealed class State : System.ICloneable
		{
			internal /*private*/ Attribute attribute;
			internal /*private*/ State next;
			
			public System.Object Clone()
			{
				State clone = new State();
				clone.attribute = (Attribute) attribute.Clone();
				
				if (next != null)
				{
					clone.next = (State) next.Clone();
				}
				
				return clone;
			}
		}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private State GetCurrentState()
		{
		    var s = currentState[0];
            if (s != null || !HasAttributes)
            {
                return s;
            }

		    var c = s = currentState[0] = new State();
		    var it = attributeImpls.Values.GetEnumerator();
		    it.MoveNext();
		    c.attribute = it.Current.Value;

            while (it.MoveNext())
            {
                c.next = new State();
                c = c.next;
                c.attribute = it.Current.Value;
            }

		    return s;
		}

        /// <summary> Resets all Attributes in this AttributeSource by calling
        /// <see cref="Attribute.Clear()" /> on each Attribute implementation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearAttributes()
		{
            for (var state = GetCurrentState(); state != null;  state = state.next)
            {
                // PERF: Fast-path will aggresively inline the calls to clear for known types
                var attribute = state.attribute;

                if (attribute is TypeAttribute)
                    ((TypeAttribute)attribute).ClearFast();
                else if (attribute is OffsetAttribute)
                    ((OffsetAttribute)attribute).ClearFast();
                else if (attribute is PositionIncrementAttribute)
                    ((PositionIncrementAttribute)attribute).ClearFast();
                else if (attribute is TermAttribute)
                    ((TermAttribute)attribute).ClearFast();
                else 
                    attribute.Clear();
            }
		}
		
		/// <summary> Captures the state of all Attributes. The return value can be passed to
		/// <see cref="RestoreState" /> to restore the state of this or another AttributeSource.
		/// </summary>
		public State CaptureState()
		{
		    var state = this.GetCurrentState();
		    return (state == null) ? null : (State) state.Clone();
		}
		
		/// <summary> Restores this state by copying the values of all attribute implementations
		/// that this state contains into the attributes implementations of the targetStream.
		/// The targetStream must contain a corresponding instance for each argument
		/// contained in this state (e.g. it is not possible to restore the state of
		/// an AttributeSource containing a TermAttribute into a AttributeSource using
		/// a Token instance as implementation).
		/// 
		/// Note that this method does not affect attributes of the targetStream
		/// that are not contained in this state. In other words, if for example
		/// the targetStream contains an OffsetAttribute, but this state doesn't, then
		/// the value of the OffsetAttribute remains unchanged. It might be desirable to
		/// reset its value to the default, in which case the caller should first
        /// call <see cref="AttributeSource.ClearAttributes()" /> on the targetStream.   
		/// </summary>
		public void RestoreState(State state)
		{
			if (state == null)
				return ;
			
			do 
			{
				if (!attributeImpls.ContainsKey(state.attribute.GetType()))
				{
					throw new System.ArgumentException("State contains an AttributeImpl that is not in this AttributeSource");
				}
				state.attribute.CopyTo(attributeImpls[state.attribute.GetType()].Value);
				state = state.next;
			}
			while (state != null);
		}
		
		public override int GetHashCode()
		{
			var code = 0;

            for (var state = GetCurrentState(); state != null; state = state.next)
            {
                code = code*31 + state.attribute.GetHashCode();
            }

            return code;
		}
		
		public  override bool Equals(System.Object obj)
		{
			if (obj == this)
			{
				return true;
			}
			
			if (obj is AttributeSource)
			{
				AttributeSource other = (AttributeSource) obj;
				
				if (HasAttributes)
				{
					if (!other.HasAttributes)
					{
						return false;
					}
					
					if (this.attributeImpls.Count != other.attributeImpls.Count)
					{
						return false;
					}
					
					// it is only equal if all attribute impls are the same in the same order
				    var thisState = this.GetCurrentState();
				    var otherState = other.GetCurrentState();
                    while (thisState != null && otherState != null)
					{
						if (otherState.attribute.GetType() != thisState.attribute.GetType() || !otherState.attribute.Equals(thisState.attribute))
						{
							return false;
						}
						thisState = thisState.next;
						otherState = otherState.next;
					}
					return true;
				}
				else
				{
					return !other.HasAttributes;
				}
			}
			else
				return false;
		}
		
		public override String ToString()
		{
            System.Text.StringBuilder sb = new System.Text.StringBuilder().Append('(');
			
			if (HasAttributes)
			{
				if (currentState[0] == null)
				{
					currentState[0] = GetCurrentState();
				}
				for (var state = currentState[0]; state != null; state = state.next)
				{
					if (state != currentState[0])
						sb.Append(',');
					sb.Append(state.attribute.ToString());
				}
			}
			return sb.Append(')').ToString();
		}
		
		/// <summary> Performs a clone of all <see cref="Attribute" /> instances returned in a new
		/// AttributeSource instance. This method can be used to e.g. create another TokenStream
		/// with exactly the same attributes (using <see cref="AttributeSource(AttributeSource)" />)
		/// </summary>
		public AttributeSource CloneAttributes()
		{
			var clone = new AttributeSource(this.factory);
			
			// first clone the impls
			if (HasAttributes)
			{
                for (var state = GetCurrentState(); state != null; state = state.next)
                {
                    var impl = (Attribute) state.attribute.Clone();

                    var type = impl.GetType();
                    if (!clone.attributes.TryGetValue(type, out var _))
                        clone.attributeImpls[type] = new AttributeImplItem(type, impl);
                }
			}
			
			// now the interfaces
            foreach (var item in this.attributes)
            {
                var att = item.Value;
                clone.attributes[att.Key] = new AttributeImplItem(att.Key, clone.attributeImpls[att.Value.GetType()].Value);
			}
			
			return clone;
		}
	}
}