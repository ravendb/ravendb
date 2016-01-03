// -----------------------------------------------------------------------
//  <copyright file="HttpMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;

namespace Raven.Abstractions.Util
{
    public static class HttpMethods
    {
        public static readonly HttpMethod Reset = new HttpMethod("RESET");

        public static readonly HttpMethod Patch = new HttpMethod("PATCH");

        public static readonly HttpMethod Eval = new HttpMethod("EVAL");

        public static readonly HttpMethod Get = HttpMethod.Get;

        public static readonly HttpMethod Post = HttpMethod.Post;

        public static readonly HttpMethod Put = HttpMethod.Put;

        public static readonly HttpMethod Delete = HttpMethod.Delete;

        public static readonly HttpMethod Head = HttpMethod.Head;
    }
}
