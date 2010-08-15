/* ****************************************************************************
 *
 * Copyright (c) Microsoft Corporation. 
 *
 * This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
 * copy of the license can be found in the License.html file at the root of this distribution. If 
 * you cannot locate the  Apache License, Version 2.0, please send an email to 
 * ironruby@microsoft.com. By using this source code in any fashion, you are agreeing to be bound 
 * by the terms of the Apache License, Version 2.0.
 *
 * You must not remove this notice, or any other, from this software.
 *
 *
 * ***************************************************************************/

using System;
using System.Diagnostics;
using System.IO;
using System.Resources;
using System.Windows;

/// <summary>
/// This assembly is used to embed a splash screen image. System.Windows.SplashScreen only supports
/// embedded images. We could avoid the need to use compiled C# code and instead generate a Ref.Emit
/// on the fly to hold the embedded image resource, but it might affect start up time.
/// 
/// Run the following commands to create SplashScreen.dll:
///   csc /t:library /r:WindowsBase.dll SplashScreen.cs
///   rbx -e "require 'SplashScreen.dll'; SplashScreen::SplashScreen.write_image_resource 'SplashScreen.png', 'SplashScreen.g.resources'"
///   csc /t:library /r:WindowsBase.dll /resource:SplashScreen.g.resources SplashScreen.cs
/// </summary>
namespace SplashScreen {
    public class SplashScreen {
        /// <summary>
        /// Creates a resources file with an image
        /// </summary>
        /// <param name="imageSourcePath"></param>
        /// <param name="outputPath"></param>
        public static void WriteImageResource(string imageSourcePath, string outputPath) {
            Debug.Assert(imageSourcePath.EndsWith(".png"));
            Debug.Assert(outputPath.EndsWith(".resources"));

            FileInfo fileInfo = new FileInfo(imageSourcePath);
            FileStream fsSource = fileInfo.OpenRead();
            byte[] bytes = new byte[fileInfo.Length + 1];
            int bytesRead = fsSource.Read(bytes, 0, bytes.Length);
            Debug.Assert(bytesRead == fileInfo.Length);
            MemoryStream memoryStream = new MemoryStream(bytes, 0, bytesRead);
            ResourceWriter resw = new ResourceWriter(outputPath);
            resw.AddResource(imageSourcePath.ToLowerInvariant(), memoryStream);
            resw.Close();
        }

        public static void Show() {
            var s = new System.Windows.SplashScreen(typeof(SplashScreen).Assembly, "splashscreen.png");
            s.Show(true);
        }
    }
}