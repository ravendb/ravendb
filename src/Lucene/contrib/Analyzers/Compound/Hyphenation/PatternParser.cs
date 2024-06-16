//using System;
//using System.Collections;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Text;
//using Lucene.Net.Analysis.Compound.Hyphenation;

//namespace Lucene.Net.Analyzers.Compound.Hyphenation
//{
// /*
// * A SAX document handler to read and parse hyphenation patterns from a XML
// * file.
// * 
// * This class has been taken from the Apache FOP project (http://xmlgraphics.apache.org/fop/). They have been slightly modified. 
// */
//public class PatternParser : DefaultHandler, PatternConsumer {

//  XMLReader parser;

//  int currElement;

//  PatternConsumer consumer;

//  StringBuilder token;

//  ArrayList exception;

//  char hyphenChar;

//  String errMsg;

//  static readonly int ELEM_CLASSES = 1;

//  static readonly int ELEM_EXCEPTIONS = 2;

//  static readonly int ELEM_PATTERNS = 3;

//  static readonly int ELEM_HYPHEN = 4;

//  public PatternParser() 
//  {
//    token = new StringBuilder();
//    parser = CreateParser();
//    parser.SetContentHandler(this);
//    parser.SetErrorHandler(this);
//    parser.SetEntityResolver(this);
//    hyphenChar = '-'; // default

//  }

//  public PatternParser(PatternConsumer consumer)
//      : this()
//  {
//    this.consumer = consumer;
//  }

//  public void setConsumer(PatternConsumer consumer) {
//    this.consumer = consumer;
//  }

//  /*
//   * Parses a hyphenation pattern file.
//   * 
//   * @param filename the filename
//   * @throws HyphenationException In case of an exception while parsing
//   */
//  public void parse(String filename) 
//  {
//    parse(new FileInfo(filename));
//  }

//  /*
//   * Parses a hyphenation pattern file.
//   * 
//   * @param file the pattern file
//   * @throws HyphenationException In case of an exception while parsing
//   */
//  public void parse(FileInfo file) 
//  {
//    try {
//      InputSource src = new InputSource(file.toURL().toExternalForm());
//      parse(src);
//    } catch (MalformedURLException e) {
//      throw new HyphenationException("Error converting the File '" + file
//          + "' to a URL: " + e.GetMessage());
//    }
//  }

//  /*
//   * Parses a hyphenation pattern file.
//   * 
//   * @param source the InputSource for the file
//   * @throws HyphenationException In case of an exception while parsing
//   */
//  public void parse(InputSource source) 
//  {
//    try {
//      parser.parse(source);
//    } catch (FileNotFoundException fnfe) {
//      throw new HyphenationException("File not found: " + fnfe.GetMessage());
//    } catch (IOException ioe) {
//      throw new HyphenationException(ioe.GetMessage());
//    } catch (SAXException e) {
//      throw new HyphenationException(errMsg);
//    }
//  }

//  /*
//   * Creates a SAX parser using JAXP
//   * 
//   * @return the created SAX parser
//   */
//  static XMLReader createParser() {
//    try {
//      SAXParserFactory factory = SAXParserFactory.newInstance();
//      factory.SetNamespaceAware(true);
//      return factory.newSAXParser().GetXMLReader();
//    } catch (Exception e) {
//      throw new RuntimeException("Couldn't create XMLReader: " + e.GetMessage());
//    }
//  }

//  protected String readToken(StringBuffer chars) {
//    String word;
//    bool space = false;
//    int i;
//    for (i = 0; i < chars.Length(); i++) {
//      if (char.isWhitespace(chars.charAt(i))) {
//        space = true;
//      } else {
//        break;
//      }
//    }
//    if (space) {
//      // chars.delete(0,i);
//      for (int countr = i; countr < chars.Length(); countr++) {
//        chars.SetCharAt(countr - i, chars.charAt(countr));
//      }
//      chars.SetLength(chars.Length() - i);
//      if (token.Length() > 0) {
//        word = token.ToString();
//        token.SetLength(0);
//        return word;
//      }
//    }
//    space = false;
//    for (i = 0; i < chars.Length(); i++) {
//      if (char.isWhitespace(chars.charAt(i))) {
//        space = true;
//        break;
//      }
//    }
//    token.Append(chars.ToString().substring(0, i));
//    // chars.delete(0,i);
//    for (int countr = i; countr < chars.Length(); countr++) {
//      chars.SetCharAt(countr - i, chars.charAt(countr));
//    }
//    chars.SetLength(chars.Length() - i);
//    if (space) {
//      word = token.ToString();
//      token.SetLength(0);
//      return word;
//    }
//    token.Append(chars);
//    return null;
//  }

//  protected static String getPattern(String word) {
//    StringBuilder pat = new StringBuilder();
//    int len = word.Length();
//    for (int i = 0; i < len; i++) {
//      if (!char.isDigit(word.charAt(i))) {
//        pat.Append(word.charAt(i));
//      }
//    }
//    return pat.ToString();
//  }

//  protected ArrayList normalizeException(ArrayList ex) {
//    ArrayList res = new ArrayList();
//    for (int i = 0; i < ex.size(); i++) {
//      Object item = ex.Get(i);
//      if (item instanceof String) {
//        String str = (String) item;
//        StringBuilder buf = new StringBuilder();
//        for (int j = 0; j < str.Length(); j++) {
//          char c = str.charAt(j);
//          if (c != hyphenChar) {
//            buf.Append(c);
//          } else {
//            res.add(buf.ToString());
//            buf.SetLength(0);
//            char[] h = new char[1];
//            h[0] = hyphenChar;
//            // we use here hyphenChar which is not necessarily
//            // the one to be printed
//            res.add(new Hyphen(new String(h), null, null));
//          }
//        }
//        if (buf.Length() > 0) {
//          res.add(buf.ToString());
//        }
//      } else {
//        res.add(item);
//      }
//    }
//    return res;
//  }

//  protected String getExceptionWord(ArrayList ex) {
//    StringBuilder res = new StringBuilder();
//    for (int i = 0; i < ex.size(); i++) {
//      Object item = ex.Get(i);
//      if (item instanceof String) {
//        res.Append((String) item);
//      } else {
//        if (((Hyphen) item).noBreak != null) {
//          res.Append(((Hyphen) item).noBreak);
//        }
//      }
//    }
//    return res.ToString();
//  }

//  protected static String getInterletterValues(String pat) {
//    StringBuilder il = new StringBuilder();
//    String word = pat + "a"; // add dummy letter to serve as sentinel
//    int len = word.Length();
//    for (int i = 0; i < len; i++) {
//      char c = word.charAt(i);
//      if (char.isDigit(c)) {
//        il.Append(c);
//        i++;
//      } else {
//        il.Append('0');
//      }
//    }
//    return il.ToString();
//  }

//  //
//  // EntityResolver methods
//  //
//  public override InputSource resolveEntity(String publicId, String systemId) {
//    return HyphenationDTDGenerator.generateDTD();
//  }

//  //
//  // ContentHandler methods
//  //

//  /*
//   * @see org.xml.sax.ContentHandler#startElement(java.lang.String,
//   *      java.lang.String, java.lang.String, org.xml.sax.Attributes)
//   */
//  public override void startElement(String uri, String local, String raw,
//      Attributes attrs) {
//    if (local.equals("hyphen-char")) {
//      String h = attrs.GetValue("value");
//      if (h != null && h.Length() == 1) {
//        hyphenChar = h.charAt(0);
//      }
//    } else if (local.equals("classes")) {
//      currElement = ELEM_CLASSES;
//    } else if (local.equals("patterns")) {
//      currElement = ELEM_PATTERNS;
//    } else if (local.equals("exceptions")) {
//      currElement = ELEM_EXCEPTIONS;
//      exception = new ArrayList();
//    } else if (local.equals("hyphen")) {
//      if (token.Length() > 0) {
//        exception.add(token.ToString());
//      }
//      exception.add(new Hyphen(attrs.GetValue("pre"), attrs.GetValue("no"),
//          attrs.GetValue("post")));
//      currElement = ELEM_HYPHEN;
//    }
//    token.SetLength(0);
//  }

//  /*
//   * @see org.xml.sax.ContentHandler#endElement(java.lang.String,
//   *      java.lang.String, java.lang.String)
//   */
//  public override void endElement(String uri, String local, String raw) {

//    if (token.Length() > 0) {
//      String word = token.ToString();
//      switch (currElement) {
//        case ELEM_CLASSES:
//          consumer.addClass(word);
//          break;
//        case ELEM_EXCEPTIONS:
//          exception.add(word);
//          exception = normalizeException(exception);
//          consumer.addException(getExceptionWord(exception),
//              (ArrayList) exception.clone());
//          break;
//        case ELEM_PATTERNS:
//          consumer.addPattern(getPattern(word), getInterletterValues(word));
//          break;
//        case ELEM_HYPHEN:
//          // nothing to do
//          break;
//      }
//      if (currElement != ELEM_HYPHEN) {
//        token.SetLength(0);
//      }
//    }
//    if (currElement == ELEM_HYPHEN) {
//      currElement = ELEM_EXCEPTIONS;
//    } else {
//      currElement = 0;
//    }

//  }

//  /*
//   * @see org.xml.sax.ContentHandler#chars(char[], int, int)
//   */
//  public override void chars(char ch[], int start, int Length) {
//    StringBuffer chars = new StringBuffer(Length);
//    chars.Append(ch, start, Length);
//    String word = readToken(chars);
//    while (word != null) {
//      // Console.WriteLine("\"" + word + "\"");
//      switch (currElement) {
//        case ELEM_CLASSES:
//          consumer.addClass(word);
//          break;
//        case ELEM_EXCEPTIONS:
//          exception.add(word);
//          exception = normalizeException(exception);
//          consumer.addException(getExceptionWord(exception),
//              (ArrayList) exception.clone());
//          exception.clear();
//          break;
//        case ELEM_PATTERNS:
//          consumer.addPattern(getPattern(word), getInterletterValues(word));
//          break;
//      }
//      word = readToken(chars);
//    }

//  }

//  //
//  // ErrorHandler methods
//  //

//  /*
//   * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
//   */
//  public override void warning(SAXParseException ex) {
//    errMsg = "[Warning] " + getLocationString(ex) + ": " + ex.GetMessage();
//  }

//  /*
//   * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
//   */
//  public override void error(SAXParseException ex) {
//    errMsg = "[Error] " + getLocationString(ex) + ": " + ex.GetMessage();
//  }

//  /*
//   * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
//   */
//  public override void fatalError(SAXParseException ex) throws SAXException {
//    errMsg = "[Fatal Error] " + getLocationString(ex) + ": " + ex.GetMessage();
//    throw ex;
//  }

//  /*
//   * Returns a string of the location.
//   */
//  private String getLocationString(SAXParseException ex) {
//    StringBuilder str = new StringBuilder();

//    String systemId = ex.GetSystemId();
//    if (systemId != null) {
//      int index = systemId.lastIndexOf('/');
//      if (index != -1) {
//        systemId = systemId.substring(index + 1);
//      }
//      str.Append(systemId);
//    }
//    str.Append(':');
//    str.Append(ex.GetLineNumber());
//    str.Append(':');
//    str.Append(ex.GetColumnNumber());

//    return str.ToString();

//  } // getLocationString(SAXParseException):String

//  // PatternConsumer implementation for testing purposes
//  public void addClass(String c) {
//    Console.WriteLine("class: " + c);
//  }

//  public void addException(String w, ArrayList e) {
//    Console.WriteLine("exception: " + w + " : " + e.ToString());
//  }

//  public void addPattern(String p, String v) {
//    Console.WriteLine("pattern: " + p + " : " + v);
//  }

//  public static void main(String[] args) 
//  {
//    if (args.Length > 0) {
//      PatternParser pp = new PatternParser();
//      pp.SetConsumer(pp);
//      pp.parse(args[0]);
//    }
//  }
//}

//class HyphenationDTDGenerator {
//  public static readonly String DTD_STRING=
//    "<?xml version=\"1.0\" encoding=\"US-ASCII\"?>\n"+
//    "<!--\n"+
//    "  Copyright 1999-2004 The Apache Software Foundation\n"+
//    "\n"+
//    "  Licensed under the Apache License, Version 2.0 (the \"License\");\n"+
//    "  you may not use this file except in compliance with the License.\n"+
//    "  You may obtain a copy of the License at\n"+
//    "\n"+
//    "       http://www.apache.org/licenses/LICENSE-2.0\n"+
//    "\n"+
//    "  Unless required by applicable law or agreed to in writing, software\n"+
//    "  distributed under the License is distributed on an \"AS IS\" BASIS,\n"+
//    "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"+
//    "  See the License for the specific language governing permissions and\n"+
//    "  limitations under the License.\n"+
//    "-->\n"+
//    "<!-- $Id: hyphenation.dtd,v 1.3 2004/02/27 18:34:59 jeremias Exp $ -->\n"+
//    "\n"+
//    "<!ELEMENT hyphenation-info (hyphen-char?, hyphen-min?,\n"+
//    "                           classes, exceptions?, patterns)>\n"+
//    "\n"+
//    "<!-- Hyphen char to be used in the exception list as shortcut for\n"+
//    "     <hyphen pre-break=\"-\"/>. Defaults to '-'\n"+
//    "-->\n"+
//    "<!ELEMENT hyphen-char EMPTY>\n"+
//    "<!ATTLIST hyphen-char value CDATA #REQUIRED>\n"+
//    "\n"+
//    "<!-- Default minimun Length in chars of hyphenated word fragments\n"+
//    "     before and after the line break. For some languages this is not\n"+
//    "     only for aesthetic purposes, wrong hyphens may be generated if this\n"+
//    "     is not accounted for.\n"+
//    "-->\n"+
//    "<!ELEMENT hyphen-min EMPTY>\n"+
//    "<!ATTLIST hyphen-min before CDATA #REQUIRED>\n"+
//    "<!ATTLIST hyphen-min after CDATA #REQUIRED>\n"+
//    "\n"+
//    "<!-- char equivalent classes: space separated list of char groups, all\n"+
//    "     chars in a group are to be treated equivalent as far as\n"+
//    "     the hyphenation algorithm is concerned. The first char in a group\n"+
//    "     is the group's equivalent char. Patterns should only contain\n"+
//    "     first chars. It also defines word chars, i.e. a word that\n"+
//    "     contains chars not present in any of the classes is not hyphenated.\n"+
//    "-->\n"+
//    "<!ELEMENT classes (#PCDATA)>\n"+
//    "\n"+
//    "<!-- Hyphenation exceptions: space separated list of hyphenated words.\n"+
//    "     A hyphen is indicated by the hyphen tag, but you can use the\n"+
//    "     hyphen-char defined previously as shortcut. This is in cases\n"+
//    "     when the algorithm procedure finds wrong hyphens or you want\n"+
//    "     to provide your own hyphenation for some words.\n"+
//    "-->\n"+
//    "<!ELEMENT exceptions (#PCDATA|hyphen)* >\n"+
//    "\n"+
//    "<!-- The hyphenation patterns, space separated. A pattern is made of 'equivalent'\n"+
//    "     chars as described before, between any two word chars a digit\n"+
//    "     in the range 0 to 9 may be specified. The absence of a digit is equivalent\n"+
//    "     to zero. The '.' char is reserved to indicate begining or ending\n"+
//    "     of words. -->\n"+
//    "<!ELEMENT patterns (#PCDATA)>\n"+
//    "\n"+
//    "<!-- A \"full hyphen\" equivalent to TeX's \\discretionary\n"+
//    "     with pre-break, post-break and no-break attributes.\n"+
//    "     To be used in the exceptions list, the hyphen char is not\n"+
//    "     automatically added -->\n"+
//    "<!ELEMENT hyphen EMPTY>\n"+
//    "<!ATTLIST hyphen pre CDATA #IMPLIED>\n"+
//    "<!ATTLIST hyphen no CDATA #IMPLIED>\n"+
//    "<!ATTLIST hyphen post CDATA #IMPLIED>\n";
  
// public static InputSource generateDTD() {
//    return new InputSource(new StringReader(DTD_STRING));
//  }
//}
//}
