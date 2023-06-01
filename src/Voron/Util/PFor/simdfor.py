#!/usr/bin/env python3

# example execution to handle formatting as well
# python.exe .\simdfor.py | &"C:\Program Files\LLVM\bin\clang-format.exe" --style Microsoft | Out-File -Encoding utf8 -FilePath .\SimdPacking.Generated.cs

from math import ceil

print("""using System.Runtime.Intrinsics;

//*************
// Generated using:
//    python.exe .\simdfor.py | clang-format --style Microsoft > .\Simd.Generated.cs
//*************

namespace Voron.Util.Simd;

unsafe partial struct SimdPacking<TTransform>
    where TTransform : struct, ISimdTransform
{


""")

def mask(bit):
  return str((1 << bit) - 1)

for length in [32]:
  print("""
    public static Vector256<uint> iunpackFOR0(Vector256<uint> initOffset, Vector256<uint>*   input, Vector256<uint>*   output) {
        var outputVec = (Vector256<uint>*)output;
        for (int i = 0; i < 8; ++i) {
              *output++ = initOffset;
              *output++ = initOffset; 
              *output++ = initOffset; 
              *output++ = initOffset;
            }

        return initOffset;
    }

   """)

  print("""
    public static void ipackFOR0(Vector256<uint>  initOffset, Vector256<uint>* input, Vector256<uint>* output) 
    {
        // nothing to do here
    }
  """) 

  for bit in range(1,33):
    offsetVar = " initOffset"
    print("""  
    public static void ipackFOR"""+str(bit)+"""(Vector256<uint> """+offsetVar+""", Vector256<uint>* input, Vector256<uint>* output) 
    {
      Vector256<uint>    outReg;
        """)
    
    if (bit != 32):
      print("      TTransform simdTransform = default;")
      print("      var currIn = *input; // __m128i CurrIn = _mm_load_si128(in);")
      print("      var inReg = simdTransform.Encode(currIn, ref initOffset); // __m128i InReg = Delta(CurrIn, initOffset);")
      print("                                             // initOffset = currIn;")
    else:
      print("      var inReg = *input; // __m128i InReg = _mm_load_si128(in);")


    inwordpointer = 0
    valuecounter = 0
    for k in range(ceil((length * bit) / 32)):
      if(valuecounter == length): break
      for x in range(inwordpointer,32,bit):
        if(x!=0) :
          print("    outReg |= Vector256.ShiftLeft(inReg, " + str(x) + " ); // OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, " + str(x) + "));")
        else:
          print("    outReg = inReg; //  OutReg = InReg; ")
        if((x+bit>=32) ):
          while(inwordpointer<32):
            inwordpointer += bit
          print("    *output = outReg; // _mm_store_si128(out, OutReg);")
          print("")
          if(valuecounter + 1 < length):
            print("    ++output; // ++out;")

          inwordpointer -= 32
          if(inwordpointer>0):
            print("    outReg = Vector256.ShiftRightLogical(inReg, " + str(bit) + " - " + str(inwordpointer) + "); // OutReg = _mm_srli_epi32(InReg, " + str(bit) + " - " + str(inwordpointer) + ")")
        if(valuecounter + 1 < length):
          print("    ++input; // ++in;") 

          if (bit != 32):
            print("    currIn = *input; // CurrIn = _mm_load_si128(in);")
            print("    inReg = simdTransform.Encode(currIn, ref initOffset); // InReg = Delta(CurrIn, initOffset);")
            print("                                           // initOffset = CurrIn; ")
          else:
            print("    inReg = *input; // InReg = _mm_load_si128(in);")
          print("")
        valuecounter = valuecounter + 1
        if(valuecounter == length): break
    assert(valuecounter == length)
    print("\n}\n\n""")

  for bit in range(1,32):
    offsetVar = " initOffset"
    print("""\n
public static void iunpackFOR"""+str(bit)+"""(Vector256<uint> """+offsetVar+""", Vector256<uint>*   input, Vector256<uint>*  output) {
      """)
    print("""    
    var inReg = *input;
    Vector256<uint>    outReg;    
    Vector256<uint>     tmp;
    TTransform simdTransform = default;
    Vector256<uint> mask =  Vector256.Create((1U<<"""+str(bit)+""")-1); // _mm_set1_epi32((1U<<"""+str(bit)+""")-1);

    """)

    MainText = ""

    MainText += "\n"
    inwordpointer = 0
    valuecounter = 0
    for k in range(ceil((length * bit) / 32)):
      for x in range(inwordpointer,32,bit):
        if(valuecounter == length): break
        if (x > 0):
          MainText += "    tmp = Vector256.ShiftRightLogical(inReg," + str(x) +"); // tmp = _mm_srli_epi32(InReg," + str(x) +")\n" 
        else:
          MainText += "    tmp = inReg; // tmp = InReg;\n" 
        if(x+bit<32):
          MainText += "    outReg = tmp & mask; // OutReg = _mm_and_si128(tmp, mask);\n"
        else:
          MainText += "    outReg = tmp; // OutReg = tmp;\n"
       
        if((x+bit>=32) ):      
          while(inwordpointer<32):
            inwordpointer += bit
          if(valuecounter + 1 < length):
             MainText += "    ++input; // ++in;\n"
             MainText += "    inReg = *input; // InReg = _mm_load_si128(in);\n"
          inwordpointer -= 32
          if(inwordpointer>0):
            MainText += "    outReg |= Vector256.ShiftLeft(inReg, " + str(bit) + "-" + str(inwordpointer) + ") & mask; //  OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, " + str(bit) + "-" + str(inwordpointer) + "), mask)); \n\n"
        if (bit != 32):
          MainText += "    outReg = simdTransform.Decode(outReg, ref initOffset); //  PrefixSum(OutReg, OutReg, initOffset);\n"
          MainText += "                                            //    initOffset = OutReg;\n" 

        MainText += "    *output++ = outReg; //_mm_store_si128(out++, OutReg);\n\n"
        MainText += ""
        valuecounter = valuecounter + 1
        if(valuecounter == length): break
    assert(valuecounter == length)
    print(MainText)
    print("\n}\n\n")
  print("""
  public  static void iunpackFOR32(Vector256<uint>  initOffset, Vector256<uint>*   input, Vector256<uint>*   output) {
    for(int k = 0; k < 256/8; ++k) {
        *output++ =  *input++;
    }
  }
}  """)
