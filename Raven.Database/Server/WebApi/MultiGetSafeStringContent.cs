// -----------------------------------------------------------------------
//  <copyright file=" MultiGetSafeStringContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;
using System.Text;

namespace Raven.Database.Server.WebApi
{
    public class  MultiGetSafeStringContent : StringContent
    {
        public string Content { get; set; }
        public MultiGetSafeStringContent(string content) : base(content)
        {
            Content = content;
        }

        public MultiGetSafeStringContent(string content, Encoding encoding) : base(content, encoding)
        {
            Content = content;
        }

        public MultiGetSafeStringContent(string content, Encoding encoding, string mediaType) : base(content, encoding, mediaType)
        {
            Content = content;
        }
    }
}