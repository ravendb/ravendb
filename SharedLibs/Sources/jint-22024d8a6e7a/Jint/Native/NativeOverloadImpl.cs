using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Jint.Native
{
    /// <summary>
    /// This class is used in the overload implementation for the NativeConstructor and NativeOverloadImplementation
    /// </summary>
    /// <typeparam name="TMemberInfo">A Member info type</typeparam>
    /// <typeparam name="TImpl">An implementation details type</typeparam>
    public class NativeOverloadImpl<TMemberInfo, TImpl>
        where TMemberInfo : MethodBase
        where TImpl : class
    {
        public delegate IEnumerable<TMemberInfo> GetMembersDelegate(Type[] genericArguments, int argCount);
        public delegate TImpl WrapMmemberDelegate(TMemberInfo info);

        Dictionary<string, TImpl> m_protoCache = new Dictionary<string, TImpl>();
        Dictionary<TMemberInfo, TImpl> m_reflectCache = new Dictionary<TMemberInfo, TImpl>();
        Marshaller m_marshaller;
        GetMembersDelegate GetMembers;
        WrapMmemberDelegate WrapMember;
        

        class MethodMatch
        {
            public TMemberInfo method;
            public int weight;
            public Type[] parameters;
        }

        public NativeOverloadImpl(Marshaller marshaller, GetMembersDelegate getMembers, WrapMmemberDelegate wrapMember)
        {
            if (marshaller == null)
                throw new ArgumentNullException("marshaller");
            if (getMembers == null)
                throw new ArgumentNullException("getMembers");
            if (wrapMember == null)
                throw new ArgumentNullException("wrapMember");

            m_marshaller = marshaller;
            GetMembers = getMembers;
            WrapMember = wrapMember;
        }

        protected TMemberInfo MatchMethod(Type[] args, IEnumerable<TMemberInfo> members)
        {
            LinkedList<MethodMatch> matches = new LinkedList<MethodMatch>();

            foreach (var m in members)
                matches.AddLast(
                    new MethodMatch()
                    {
                        method = m,
                        parameters = Array.ConvertAll<ParameterInfo, Type>(
                            m.GetParameters(),
                            p => p.ParameterType
                        ),
                        weight = 0
                    }
                );
            

            if (args != null)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    Type t = args[i];
                    for (var node = matches.First; node != null; )
                    {
                        var nextNode = node.Next;
                        if (t != null)
                        {
                            Type paramType = node.Value.parameters[i];
                            if (t.Equals(paramType))
                            {
                                node.Value.weight += 1;
                            }
                            else if (typeof(Delegate).IsAssignableFrom(paramType) && typeof(JsFunction).IsAssignableFrom(t))
                            {
                                // we can assing a js function to a delegate
                            }
                            else if (!m_marshaller.IsAssignable(paramType,t))
                            {
                                matches.Remove(node);
                            }

                        }
                        else
                        {
                            // we can't assign undefined or null values to a value types
                            if (node.Value.parameters[i].IsValueType)
                            {
                                matches.Remove(node);
                            }
                        }
                        node = nextNode;
                    }
                }
            }

            MethodMatch best = null;

            foreach (var match in matches)
                best = best == null ? match : (best.weight < match.weight ? match : best);

            return best == null ? null : best.method ;
        }

        protected string MakeKey(Type[] types, Type[] genericArguments)
        {
            return
                "<"
                + String.Join(
                    ",",
                    Array.ConvertAll<Type, string>(
                        genericArguments ?? new Type[0],
                        t => t == null ? "<null>" : t.FullName
                    )
                )
                + ">"
                + String.Join(
                    ",",
                    Array.ConvertAll<Type, String>(
                        types ?? new Type[0],
                        t => t == null ? "<null>" : t.FullName
                    )
                );
        }

        public void DefineCustomOverload(Type[] args, Type[] generics, TImpl impl)
        {
            m_protoCache[MakeKey(args, generics)] = impl;
        }

        public TImpl ResolveOverload(JsInstance[] args, Type[] generics)
        {
            Type[] argTypes = Array.ConvertAll<JsInstance, Type>(args, x => m_marshaller.GetInstanceType(x));
            string key = MakeKey(argTypes, generics);
            TImpl method;
            if (!m_protoCache.TryGetValue(key, out method))
            {
                TMemberInfo info = MatchMethod(argTypes, GetMembers(generics,args.Length) );

                if (info != null && !m_reflectCache.TryGetValue(info, out method))
                    m_reflectCache[info] = method = WrapMember(info);

                m_protoCache[key] = method;
            }

            return method;
        }
    }
}
