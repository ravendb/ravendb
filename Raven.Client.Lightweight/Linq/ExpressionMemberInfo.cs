using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace Raven.Client.Linq
{
    /// <summary>
    /// This class represents a node in an expression, usually a member - but in the case of dynamic queries the path to a member
    /// </summary>
    public class ExpressionMemberInfo
    {
        /// <summary>
        /// Gets the full path of the member being referred to by this node
        /// </summary>
        public string Path
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the actual member being referred to
        /// </summary>
        public MemberInfo InnerMemberInfo
        {
            get;
            private set;
        }

        /// <summary>
        /// Creates an ExpressionMemberInfo
        /// </summary>
        /// <param name="path"></param>
        /// <param name="innerMemberInfo"></param>
        public ExpressionMemberInfo(string path, MemberInfo innerMemberInfo)
        {
            this.Path = path;
            this.InnerMemberInfo = innerMemberInfo;
        }
    }
}
