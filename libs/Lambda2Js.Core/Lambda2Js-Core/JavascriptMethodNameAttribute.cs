using System;

namespace Lambda2Js
{
    public class JavascriptMethodNameAttribute : Attribute
    {
        public string Name;

        public JavascriptMethodNameAttribute(string name)
        {
            Name = name;
        }
    }
}