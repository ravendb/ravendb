using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;

namespace Jint.PropertyBags
{
    public class ScopedPropertyBag : IPropertyBag
    {
        public void EnterScope()
        {
            currentScope = new List<Stack<Descriptor>>();
            scopes.Push(currentScope);
        }

        public void ExitScope()
        {
            foreach (Stack<Descriptor> desc in currentScope)
            {
                desc.Pop();
            }
            scopes.Pop();
            currentScope = scopes.Peek();
        }

        Dictionary<string, Stack<Descriptor>> bag = new Dictionary<string, Stack<Descriptor>>();
        Stack<List<Stack<Descriptor>>> scopes = new Stack<List<Stack<Descriptor>>>();
        List<Stack<Descriptor>> currentScope;

        #region IPropertyBag Members

        public Jint.Native.Descriptor Put(string name, Jint.Native.Descriptor descriptor)
        {
            Stack<Descriptor> stack;
            if (!bag.TryGetValue(name, out stack))
            {
                stack = new Stack<Descriptor>();
                bag.Add(name, stack);
            }
            stack.Push(descriptor);
            currentScope.Add(stack);
            return descriptor;
        }

        public void Delete(string name)
        {
            Stack<Descriptor> stack;
            if (bag.TryGetValue(name, out stack) && currentScope.Contains(stack))
            {
                stack.Pop();
                currentScope.Remove(stack);
            }

        }

        public Jint.Native.Descriptor Get(string name)
        {
            Stack<Descriptor> stack;
            if (bag.TryGetValue(name, out stack))
                return stack.Count > 0 ? stack.Peek() : null;
            return null;
        }

        public bool TryGet(string name, out Jint.Native.Descriptor descriptor)
        {
            descriptor = Get(name);
            return descriptor != null;
        }

        public int Count
        {
            get { return bag.Count; }
        }

        public IEnumerable<Jint.Native.Descriptor> Values
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

        #region IEnumerable<KeyValuePair<string,Descriptor>> Members

        public IEnumerator<KeyValuePair<string, Jint.Native.Descriptor>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
