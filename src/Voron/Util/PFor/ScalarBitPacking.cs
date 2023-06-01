using System;
using System.Runtime.CompilerServices;

namespace Voron.Util.PFor;

// Adapted from: https://github.com/lemire/JavaFastPFOR
public static unsafe class ScalarBitPacking {

  public static void MaskedPack32(int *input, int *output, int bit) {
    switch (bit) {
    case 0:
      fastpack0(input, output);
      break;
    case 1:
      fastpack1(input, output);
      break;
    case 2:
      fastpack2(input, output);
      break;
    case 3:
      fastpack3(input, output);
      break;
    case 4:
      fastpack4(input, output);
      break;
    case 5:
      fastpack5(input, output);
      break;
    case 6:
      fastpack6(input, output);
      break;
    case 7:
      fastpack7(input, output);
      break;
    case 8:
      fastpack8(input, output);
      break;
    case 9:
      fastpack9(input, output);
      break;
    case 10:
      fastpack10(input, output);
      break;
    case 11:
      fastpack11(input, output);
      break;
    case 12:
      fastpack12(input, output);
      break;
    case 13:
      fastpack13(input, output);
      break;
    case 14:
      fastpack14(input, output);
      break;
    case 15:
      fastpack15(input, output);
      break;
    case 16:
      fastpack16(input, output);
      break;
    case 17:
      fastpack17(input, output);
      break;
    case 18:
      fastpack18(input, output);
      break;
    case 19:
      fastpack19(input, output);
      break;
    case 20:
      fastpack20(input, output);
      break;
    case 21:
      fastpack21(input, output);
      break;
    case 22:
      fastpack22(input, output);
      break;
    case 23:
      fastpack23(input, output);
      break;
    case 24:
      fastpack24(input, output);
      break;
    case 25:
      fastpack25(input, output);
      break;
    case 26:
      fastpack26(input, output);
      break;
    case 27:
      fastpack27(input, output);
      break;
    case 28:
      fastpack28(input, output);
      break;
    case 29:
      fastpack29(input, output);
      break;
    case 30:
      fastpack30(input, output);
      break;
    case 31:
      fastpack31(input, output);
      break;
    case 32:
      fastpack32(input, output);
      break;
    default:
      throw new NotSupportedException("Unsupported bit width.");
    }
  }

  private static void fastpack0(int *input, int *output) {
    // nothing
  }

  private static void fastpack1(int *input, int *output) {
    output[0] =
        (input[0] & 1) | ((input[1] & 1) << 1) | ((input[2] & 1) << 2) |
        ((input[3] & 1) << 3) | ((input[4] & 1) << 4) | ((input[5] & 1) << 5) |
        ((input[6] & 1) << 6) | ((input[7] & 1) << 7) | ((input[8] & 1) << 8) |
        ((input[9] & 1) << 9) | ((input[10] & 1) << 10) |
        ((input[11] & 1) << 11) | ((input[12] & 1) << 12) |
        ((input[13] & 1) << 13) | ((input[14] & 1) << 14) |
        ((input[15] & 1) << 15) | ((input[16] & 1) << 16) |
        ((input[17] & 1) << 17) | ((input[18] & 1) << 18) |
        ((input[19] & 1) << 19) | ((input[20] & 1) << 20) |
        ((input[21] & 1) << 21) | ((input[22] & 1) << 22) |
        ((input[23] & 1) << 23) | ((input[24] & 1) << 24) |
        ((input[25] & 1) << 25) | ((input[26] & 1) << 26) |
        ((input[27] & 1) << 27) | ((input[28] & 1) << 28) |
        ((input[29] & 1) << 29) | ((input[30] & 1) << 30) | ((input[31]) << 31);
  }

  private static void fastpack10(int *input, int *output) {
    output[0] = (input[0] & 1023) | ((input[1] & 1023) << 10) |
                ((input[2] & 1023) << 20) | ((input[3]) << 30);
    output[1] = ((input[3] & 1023) >>> (10 - 8)) | ((input[4] & 1023) << 8) |
                ((input[5] & 1023) << 18) | ((input[6]) << 28);
    output[2] = ((input[6] & 1023) >>> (10 - 6)) | ((input[7] & 1023) << 6) |
                ((input[8] & 1023) << 16) | ((input[9]) << 26);
    output[3] = ((input[9] & 1023) >>> (10 - 4)) | ((input[10] & 1023) << 4) |
                ((input[11] & 1023) << 14) | ((input[12]) << 24);
    output[4] = ((input[12] & 1023) >>> (10 - 2)) | ((input[13] & 1023) << 2) |
                ((input[14] & 1023) << 12) | ((input[15]) << 22);
    output[5] = (input[16] & 1023) | ((input[17] & 1023) << 10) |
                ((input[18] & 1023) << 20) | ((input[19]) << 30);
    output[6] = ((input[19] & 1023) >>> (10 - 8)) | ((input[20] & 1023) << 8) |
                ((input[21] & 1023) << 18) | ((input[22]) << 28);
    output[7] = ((input[22] & 1023) >>> (10 - 6)) | ((input[23] & 1023) << 6) |
                ((input[24] & 1023) << 16) | ((input[25]) << 26);
    output[8] = ((input[25] & 1023) >>> (10 - 4)) | ((input[26] & 1023) << 4) |
                ((input[27] & 1023) << 14) | ((input[28]) << 24);
    output[9] = ((input[28] & 1023) >>> (10 - 2)) | ((input[29] & 1023) << 2) |
                ((input[30] & 1023) << 12) | ((input[31]) << 22);
  }

  private static void fastpack11(int *input, int *output) {
    output[0] =
        (input[0] & 2047) | ((input[1] & 2047) << 11) | ((input[2]) << 22);
    output[1] = ((input[2] & 2047) >>> (11 - 1)) | ((input[3] & 2047) << 1) |
                ((input[4] & 2047) << 12) | ((input[5]) << 23);
    output[2] = ((input[5] & 2047) >>> (11 - 2)) | ((input[6] & 2047) << 2) |
                ((input[7] & 2047) << 13) | ((input[8]) << 24);
    output[3] = ((input[8] & 2047) >>> (11 - 3)) | ((input[9] & 2047) << 3) |
                ((input[10] & 2047) << 14) | ((input[11]) << 25);
    output[4] = ((input[11] & 2047) >>> (11 - 4)) | ((input[12] & 2047) << 4) |
                ((input[13] & 2047) << 15) | ((input[14]) << 26);
    output[5] = ((input[14] & 2047) >>> (11 - 5)) | ((input[15] & 2047) << 5) |
                ((input[16] & 2047) << 16) | ((input[17]) << 27);
    output[6] = ((input[17] & 2047) >>> (11 - 6)) | ((input[18] & 2047) << 6) |
                ((input[19] & 2047) << 17) | ((input[20]) << 28);
    output[7] = ((input[20] & 2047) >>> (11 - 7)) | ((input[21] & 2047) << 7) |
                ((input[22] & 2047) << 18) | ((input[23]) << 29);
    output[8] = ((input[23] & 2047) >>> (11 - 8)) | ((input[24] & 2047) << 8) |
                ((input[25] & 2047) << 19) | ((input[26]) << 30);
    output[9] = ((input[26] & 2047) >>> (11 - 9)) | ((input[27] & 2047) << 9) |
                ((input[28] & 2047) << 20) | ((input[29]) << 31);
    output[10] = ((input[29] & 2047) >>> (11 - 10)) |
                 ((input[30] & 2047) << 10) | ((input[31]) << 21);
  }

  private static void fastpack12(int *input, int *output) {
    output[0] =
        (input[0] & 4095) | ((input[1] & 4095) << 12) | ((input[2]) << 24);
    output[1] = ((input[2] & 4095) >>> (12 - 4)) | ((input[3] & 4095) << 4) |
                ((input[4] & 4095) << 16) | ((input[5]) << 28);
    output[2] = ((input[5] & 4095) >>> (12 - 8)) | ((input[6] & 4095) << 8) |
                ((input[7]) << 20);
    output[3] =
        (input[8] & 4095) | ((input[9] & 4095) << 12) | ((input[10]) << 24);
    output[4] = ((input[10] & 4095) >>> (12 - 4)) | ((input[11] & 4095) << 4) |
                ((input[12] & 4095) << 16) | ((input[13]) << 28);
    output[5] = ((input[13] & 4095) >>> (12 - 8)) | ((input[14] & 4095) << 8) |
                ((input[15]) << 20);
    output[6] =
        (input[16] & 4095) | ((input[17] & 4095) << 12) | ((input[18]) << 24);
    output[7] = ((input[18] & 4095) >>> (12 - 4)) | ((input[19] & 4095) << 4) |
                ((input[20] & 4095) << 16) | ((input[21]) << 28);
    output[8] = ((input[21] & 4095) >>> (12 - 8)) | ((input[22] & 4095) << 8) |
                ((input[23]) << 20);
    output[9] =
        (input[24] & 4095) | ((input[25] & 4095) << 12) | ((input[26]) << 24);
    output[10] = ((input[26] & 4095) >>> (12 - 4)) | ((input[27] & 4095) << 4) |
                 ((input[28] & 4095) << 16) | ((input[29]) << 28);
    output[11] = ((input[29] & 4095) >>> (12 - 8)) | ((input[30] & 4095) << 8) |
                 ((input[31]) << 20);
  }

  private static void fastpack13(int *input, int *output) {
    output[0] =
        (input[0] & 8191) | ((input[1] & 8191) << 13) | ((input[2]) << 26);
    output[1] = ((input[2] & 8191) >>> (13 - 7)) | ((input[3] & 8191) << 7) |
                ((input[4]) << 20);
    output[2] = ((input[4] & 8191) >>> (13 - 1)) | ((input[5] & 8191) << 1) |
                ((input[6] & 8191) << 14) | ((input[7]) << 27);
    output[3] = ((input[7] & 8191) >>> (13 - 8)) | ((input[8] & 8191) << 8) |
                ((input[9]) << 21);
    output[4] = ((input[9] & 8191) >>> (13 - 2)) | ((input[10] & 8191) << 2) |
                ((input[11] & 8191) << 15) | ((input[12]) << 28);
    output[5] = ((input[12] & 8191) >>> (13 - 9)) | ((input[13] & 8191) << 9) |
                ((input[14]) << 22);
    output[6] = ((input[14] & 8191) >>> (13 - 3)) | ((input[15] & 8191) << 3) |
                ((input[16] & 8191) << 16) | ((input[17]) << 29);
    output[7] = ((input[17] & 8191) >>> (13 - 10)) |
                ((input[18] & 8191) << 10) | ((input[19]) << 23);
    output[8] = ((input[19] & 8191) >>> (13 - 4)) | ((input[20] & 8191) << 4) |
                ((input[21] & 8191) << 17) | ((input[22]) << 30);
    output[9] = ((input[22] & 8191) >>> (13 - 11)) |
                ((input[23] & 8191) << 11) | ((input[24]) << 24);
    output[10] = ((input[24] & 8191) >>> (13 - 5)) | ((input[25] & 8191) << 5) |
                 ((input[26] & 8191) << 18) | ((input[27]) << 31);
    output[11] = ((input[27] & 8191) >>> (13 - 12)) |
                 ((input[28] & 8191) << 12) | ((input[29]) << 25);
    output[12] = ((input[29] & 8191) >>> (13 - 6)) | ((input[30] & 8191) << 6) |
                 ((input[31]) << 19);
  }

  private static void fastpack14(int *input, int *output) {
    output[0] =
        (input[0] & 16383) | ((input[1] & 16383) << 14) | ((input[2]) << 28);
    output[1] = ((input[2] & 16383) >>> (14 - 10)) |
                ((input[3] & 16383) << 10) | ((input[4]) << 24);
    output[2] = ((input[4] & 16383) >>> (14 - 6)) | ((input[5] & 16383) << 6) |
                ((input[6]) << 20);
    output[3] = ((input[6] & 16383) >>> (14 - 2)) | ((input[7] & 16383) << 2) |
                ((input[8] & 16383) << 16) | ((input[9]) << 30);
    output[4] = ((input[9] & 16383) >>> (14 - 12)) |
                ((input[10] & 16383) << 12) | ((input[11]) << 26);
    output[5] = ((input[11] & 16383) >>> (14 - 8)) |
                ((input[12] & 16383) << 8) | ((input[13]) << 22);
    output[6] = ((input[13] & 16383) >>> (14 - 4)) |
                ((input[14] & 16383) << 4) | ((input[15]) << 18);
    output[7] =
        (input[16] & 16383) | ((input[17] & 16383) << 14) | ((input[18]) << 28);
    output[8] = ((input[18] & 16383) >>> (14 - 10)) |
                ((input[19] & 16383) << 10) | ((input[20]) << 24);
    output[9] = ((input[20] & 16383) >>> (14 - 6)) |
                ((input[21] & 16383) << 6) | ((input[22]) << 20);
    output[10] = ((input[22] & 16383) >>> (14 - 2)) |
                 ((input[23] & 16383) << 2) | ((input[24] & 16383) << 16) |
                 ((input[25]) << 30);
    output[11] = ((input[25] & 16383) >>> (14 - 12)) |
                 ((input[26] & 16383) << 12) | ((input[27]) << 26);
    output[12] = ((input[27] & 16383) >>> (14 - 8)) |
                 ((input[28] & 16383) << 8) | ((input[29]) << 22);
    output[13] = ((input[29] & 16383) >>> (14 - 4)) |
                 ((input[30] & 16383) << 4) | ((input[31]) << 18);
  }

  private static void fastpack15(int *input, int *output) {
    output[0] =
        (input[0] & 32767) | ((input[1] & 32767) << 15) | ((input[2]) << 30);
    output[1] = ((input[2] & 32767) >>> (15 - 13)) |
                ((input[3] & 32767) << 13) | ((input[4]) << 28);
    output[2] = ((input[4] & 32767) >>> (15 - 11)) |
                ((input[5] & 32767) << 11) | ((input[6]) << 26);
    output[3] = ((input[6] & 32767) >>> (15 - 9)) | ((input[7] & 32767) << 9) |
                ((input[8]) << 24);
    output[4] = ((input[8] & 32767) >>> (15 - 7)) | ((input[9] & 32767) << 7) |
                ((input[10]) << 22);
    output[5] = ((input[10] & 32767) >>> (15 - 5)) |
                ((input[11] & 32767) << 5) | ((input[12]) << 20);
    output[6] = ((input[12] & 32767) >>> (15 - 3)) |
                ((input[13] & 32767) << 3) | ((input[14]) << 18);
    output[7] = ((input[14] & 32767) >>> (15 - 1)) |
                ((input[15] & 32767) << 1) | ((input[16] & 32767) << 16) |
                ((input[17]) << 31);
    output[8] = ((input[17] & 32767) >>> (15 - 14)) |
                ((input[18] & 32767) << 14) | ((input[19]) << 29);
    output[9] = ((input[19] & 32767) >>> (15 - 12)) |
                ((input[20] & 32767) << 12) | ((input[21]) << 27);
    output[10] = ((input[21] & 32767) >>> (15 - 10)) |
                 ((input[22] & 32767) << 10) | ((input[23]) << 25);
    output[11] = ((input[23] & 32767) >>> (15 - 8)) |
                 ((input[24] & 32767) << 8) | ((input[25]) << 23);
    output[12] = ((input[25] & 32767) >>> (15 - 6)) |
                 ((input[26] & 32767) << 6) | ((input[27]) << 21);
    output[13] = ((input[27] & 32767) >>> (15 - 4)) |
                 ((input[28] & 32767) << 4) | ((input[29]) << 19);
    output[14] = ((input[29] & 32767) >>> (15 - 2)) |
                 ((input[30] & 32767) << 2) | ((input[31]) << 17);
  }

  private static void fastpack16(int *input, int *output) {
    output[0] = (input[0] & 65535) | ((input[1]) << 16);
    output[1] = (input[2] & 65535) | ((input[3]) << 16);
    output[2] = (input[4] & 65535) | ((input[5]) << 16);
    output[3] = (input[6] & 65535) | ((input[7]) << 16);
    output[4] = (input[8] & 65535) | ((input[9]) << 16);
    output[5] = (input[10] & 65535) | ((input[11]) << 16);
    output[6] = (input[12] & 65535) | ((input[13]) << 16);
    output[7] = (input[14] & 65535) | ((input[15]) << 16);
    output[8] = (input[16] & 65535) | ((input[17]) << 16);
    output[9] = (input[18] & 65535) | ((input[19]) << 16);
    output[10] = (input[20] & 65535) | ((input[21]) << 16);
    output[11] = (input[22] & 65535) | ((input[23]) << 16);
    output[12] = (input[24] & 65535) | ((input[25]) << 16);
    output[13] = (input[26] & 65535) | ((input[27]) << 16);
    output[14] = (input[28] & 65535) | ((input[29]) << 16);
    output[15] = (input[30] & 65535) | ((input[31]) << 16);
  }

  private static void fastpack17(int *input, int *output) {
    output[0] = (input[0] & 131071) | ((input[1]) << 17);
    output[1] = ((input[1] & 131071) >>> (17 - 2)) |
                ((input[2] & 131071) << 2) | ((input[3]) << 19);
    output[2] = ((input[3] & 131071) >>> (17 - 4)) |
                ((input[4] & 131071) << 4) | ((input[5]) << 21);
    output[3] = ((input[5] & 131071) >>> (17 - 6)) |
                ((input[6] & 131071) << 6) | ((input[7]) << 23);
    output[4] = ((input[7] & 131071) >>> (17 - 8)) |
                ((input[8] & 131071) << 8) | ((input[9]) << 25);
    output[5] = ((input[9] & 131071) >>> (17 - 10)) |
                ((input[10] & 131071) << 10) | ((input[11]) << 27);
    output[6] = ((input[11] & 131071) >>> (17 - 12)) |
                ((input[12] & 131071) << 12) | ((input[13]) << 29);
    output[7] = ((input[13] & 131071) >>> (17 - 14)) |
                ((input[14] & 131071) << 14) | ((input[15]) << 31);
    output[8] = ((input[15] & 131071) >>> (17 - 16)) | ((input[16]) << 16);
    output[9] = ((input[16] & 131071) >>> (17 - 1)) |
                ((input[17] & 131071) << 1) | ((input[18]) << 18);
    output[10] = ((input[18] & 131071) >>> (17 - 3)) |
                 ((input[19] & 131071) << 3) | ((input[20]) << 20);
    output[11] = ((input[20] & 131071) >>> (17 - 5)) |
                 ((input[21] & 131071) << 5) | ((input[22]) << 22);
    output[12] = ((input[22] & 131071) >>> (17 - 7)) |
                 ((input[23] & 131071) << 7) | ((input[24]) << 24);
    output[13] = ((input[24] & 131071) >>> (17 - 9)) |
                 ((input[25] & 131071) << 9) | ((input[26]) << 26);
    output[14] = ((input[26] & 131071) >>> (17 - 11)) |
                 ((input[27] & 131071) << 11) | ((input[28]) << 28);
    output[15] = ((input[28] & 131071) >>> (17 - 13)) |
                 ((input[29] & 131071) << 13) | ((input[30]) << 30);
    output[16] = ((input[30] & 131071) >>> (17 - 15)) | ((input[31]) << 15);
  }

  private static void fastpack18(int *input, int *output) {
    output[0] = (input[0] & 262143) | ((input[1]) << 18);
    output[1] = ((input[1] & 262143) >>> (18 - 4)) |
                ((input[2] & 262143) << 4) | ((input[3]) << 22);
    output[2] = ((input[3] & 262143) >>> (18 - 8)) |
                ((input[4] & 262143) << 8) | ((input[5]) << 26);
    output[3] = ((input[5] & 262143) >>> (18 - 12)) |
                ((input[6] & 262143) << 12) | ((input[7]) << 30);
    output[4] = ((input[7] & 262143) >>> (18 - 16)) | ((input[8]) << 16);
    output[5] = ((input[8] & 262143) >>> (18 - 2)) |
                ((input[9] & 262143) << 2) | ((input[10]) << 20);
    output[6] = ((input[10] & 262143) >>> (18 - 6)) |
                ((input[11] & 262143) << 6) | ((input[12]) << 24);
    output[7] = ((input[12] & 262143) >>> (18 - 10)) |
                ((input[13] & 262143) << 10) | ((input[14]) << 28);
    output[8] = ((input[14] & 262143) >>> (18 - 14)) | ((input[15]) << 14);
    output[9] = (input[16] & 262143) | ((input[17]) << 18);
    output[10] = ((input[17] & 262143) >>> (18 - 4)) |
                 ((input[18] & 262143) << 4) | ((input[19]) << 22);
    output[11] = ((input[19] & 262143) >>> (18 - 8)) |
                 ((input[20] & 262143) << 8) | ((input[21]) << 26);
    output[12] = ((input[21] & 262143) >>> (18 - 12)) |
                 ((input[22] & 262143) << 12) | ((input[23]) << 30);
    output[13] = ((input[23] & 262143) >>> (18 - 16)) | ((input[24]) << 16);
    output[14] = ((input[24] & 262143) >>> (18 - 2)) |
                 ((input[25] & 262143) << 2) | ((input[26]) << 20);
    output[15] = ((input[26] & 262143) >>> (18 - 6)) |
                 ((input[27] & 262143) << 6) | ((input[28]) << 24);
    output[16] = ((input[28] & 262143) >>> (18 - 10)) |
                 ((input[29] & 262143) << 10) | ((input[30]) << 28);
    output[17] = ((input[30] & 262143) >>> (18 - 14)) | ((input[31]) << 14);
  }

  private static void fastpack19(int *input, int *output) {
    output[0] = (input[0] & 524287) | ((input[1]) << 19);
    output[1] = ((input[1] & 524287) >>> (19 - 6)) |
                ((input[2] & 524287) << 6) | ((input[3]) << 25);
    output[2] = ((input[3] & 524287) >>> (19 - 12)) |
                ((input[4] & 524287) << 12) | ((input[5]) << 31);
    output[3] = ((input[5] & 524287) >>> (19 - 18)) | ((input[6]) << 18);
    output[4] = ((input[6] & 524287) >>> (19 - 5)) |
                ((input[7] & 524287) << 5) | ((input[8]) << 24);
    output[5] = ((input[8] & 524287) >>> (19 - 11)) |
                ((input[9] & 524287) << 11) | ((input[10]) << 30);
    output[6] = ((input[10] & 524287) >>> (19 - 17)) | ((input[11]) << 17);
    output[7] = ((input[11] & 524287) >>> (19 - 4)) |
                ((input[12] & 524287) << 4) | ((input[13]) << 23);
    output[8] = ((input[13] & 524287) >>> (19 - 10)) |
                ((input[14] & 524287) << 10) | ((input[15]) << 29);
    output[9] = ((input[15] & 524287) >>> (19 - 16)) | ((input[16]) << 16);
    output[10] = ((input[16] & 524287) >>> (19 - 3)) |
                 ((input[17] & 524287) << 3) | ((input[18]) << 22);
    output[11] = ((input[18] & 524287) >>> (19 - 9)) |
                 ((input[19] & 524287) << 9) | ((input[20]) << 28);
    output[12] = ((input[20] & 524287) >>> (19 - 15)) | ((input[21]) << 15);
    output[13] = ((input[21] & 524287) >>> (19 - 2)) |
                 ((input[22] & 524287) << 2) | ((input[23]) << 21);
    output[14] = ((input[23] & 524287) >>> (19 - 8)) |
                 ((input[24] & 524287) << 8) | ((input[25]) << 27);
    output[15] = ((input[25] & 524287) >>> (19 - 14)) | ((input[26]) << 14);
    output[16] = ((input[26] & 524287) >>> (19 - 1)) |
                 ((input[27] & 524287) << 1) | ((input[28]) << 20);
    output[17] = ((input[28] & 524287) >>> (19 - 7)) |
                 ((input[29] & 524287) << 7) | ((input[30]) << 26);
    output[18] = ((input[30] & 524287) >>> (19 - 13)) | ((input[31]) << 13);
  }

  private static void fastpack2(int *input, int *output) {
    output[0] = (input[0] & 3) | ((input[1] & 3) << 2) | ((input[2] & 3) << 4) |
                ((input[3] & 3) << 6) | ((input[4] & 3) << 8) |
                ((input[5] & 3) << 10) | ((input[6] & 3) << 12) |
                ((input[7] & 3) << 14) | ((input[8] & 3) << 16) |
                ((input[9] & 3) << 18) | ((input[10] & 3) << 20) |
                ((input[11] & 3) << 22) | ((input[12] & 3) << 24) |
                ((input[13] & 3) << 26) | ((input[14] & 3) << 28) |
                ((input[15]) << 30);
    output[1] = (input[16] & 3) | ((input[17] & 3) << 2) |
                ((input[18] & 3) << 4) | ((input[19] & 3) << 6) |
                ((input[20] & 3) << 8) | ((input[21] & 3) << 10) |
                ((input[22] & 3) << 12) | ((input[23] & 3) << 14) |
                ((input[24] & 3) << 16) | ((input[25] & 3) << 18) |
                ((input[26] & 3) << 20) | ((input[27] & 3) << 22) |
                ((input[28] & 3) << 24) | ((input[29] & 3) << 26) |
                ((input[30] & 3) << 28) | ((input[31]) << 30);
  }

  private static void fastpack20(int *input, int *output) {
    output[0] = (input[0] & 1048575) | ((input[1]) << 20);
    output[1] = ((input[1] & 1048575) >>> (20 - 8)) |
                ((input[2] & 1048575) << 8) | ((input[3]) << 28);
    output[2] = ((input[3] & 1048575) >>> (20 - 16)) | ((input[4]) << 16);
    output[3] = ((input[4] & 1048575) >>> (20 - 4)) |
                ((input[5] & 1048575) << 4) | ((input[6]) << 24);
    output[4] = ((input[6] & 1048575) >>> (20 - 12)) | ((input[7]) << 12);
    output[5] = (input[8] & 1048575) | ((input[9]) << 20);
    output[6] = ((input[9] & 1048575) >>> (20 - 8)) |
                ((input[10] & 1048575) << 8) | ((input[11]) << 28);
    output[7] = ((input[11] & 1048575) >>> (20 - 16)) | ((input[12]) << 16);
    output[8] = ((input[12] & 1048575) >>> (20 - 4)) |
                ((input[13] & 1048575) << 4) | ((input[14]) << 24);
    output[9] = ((input[14] & 1048575) >>> (20 - 12)) | ((input[15]) << 12);
    output[10] = (input[16] & 1048575) | ((input[17]) << 20);
    output[11] = ((input[17] & 1048575) >>> (20 - 8)) |
                 ((input[18] & 1048575) << 8) | ((input[19]) << 28);
    output[12] = ((input[19] & 1048575) >>> (20 - 16)) | ((input[20]) << 16);
    output[13] = ((input[20] & 1048575) >>> (20 - 4)) |
                 ((input[21] & 1048575) << 4) | ((input[22]) << 24);
    output[14] = ((input[22] & 1048575) >>> (20 - 12)) | ((input[23]) << 12);
    output[15] = (input[24] & 1048575) | ((input[25]) << 20);
    output[16] = ((input[25] & 1048575) >>> (20 - 8)) |
                 ((input[26] & 1048575) << 8) | ((input[27]) << 28);
    output[17] = ((input[27] & 1048575) >>> (20 - 16)) | ((input[28]) << 16);
    output[18] = ((input[28] & 1048575) >>> (20 - 4)) |
                 ((input[29] & 1048575) << 4) | ((input[30]) << 24);
    output[19] = ((input[30] & 1048575) >>> (20 - 12)) | ((input[31]) << 12);
  }

  private static void fastpack21(int *input, int *output) {
    output[0] = (input[0] & 2097151) | ((input[1]) << 21);
    output[1] = ((input[1] & 2097151) >>> (21 - 10)) |
                ((input[2] & 2097151) << 10) | ((input[3]) << 31);
    output[2] = ((input[3] & 2097151) >>> (21 - 20)) | ((input[4]) << 20);
    output[3] = ((input[4] & 2097151) >>> (21 - 9)) |
                ((input[5] & 2097151) << 9) | ((input[6]) << 30);
    output[4] = ((input[6] & 2097151) >>> (21 - 19)) | ((input[7]) << 19);
    output[5] = ((input[7] & 2097151) >>> (21 - 8)) |
                ((input[8] & 2097151) << 8) | ((input[9]) << 29);
    output[6] = ((input[9] & 2097151) >>> (21 - 18)) | ((input[10]) << 18);
    output[7] = ((input[10] & 2097151) >>> (21 - 7)) |
                ((input[11] & 2097151) << 7) | ((input[12]) << 28);
    output[8] = ((input[12] & 2097151) >>> (21 - 17)) | ((input[13]) << 17);
    output[9] = ((input[13] & 2097151) >>> (21 - 6)) |
                ((input[14] & 2097151) << 6) | ((input[15]) << 27);
    output[10] = ((input[15] & 2097151) >>> (21 - 16)) | ((input[16]) << 16);
    output[11] = ((input[16] & 2097151) >>> (21 - 5)) |
                 ((input[17] & 2097151) << 5) | ((input[18]) << 26);
    output[12] = ((input[18] & 2097151) >>> (21 - 15)) | ((input[19]) << 15);
    output[13] = ((input[19] & 2097151) >>> (21 - 4)) |
                 ((input[20] & 2097151) << 4) | ((input[21]) << 25);
    output[14] = ((input[21] & 2097151) >>> (21 - 14)) | ((input[22]) << 14);
    output[15] = ((input[22] & 2097151) >>> (21 - 3)) |
                 ((input[23] & 2097151) << 3) | ((input[24]) << 24);
    output[16] = ((input[24] & 2097151) >>> (21 - 13)) | ((input[25]) << 13);
    output[17] = ((input[25] & 2097151) >>> (21 - 2)) |
                 ((input[26] & 2097151) << 2) | ((input[27]) << 23);
    output[18] = ((input[27] & 2097151) >>> (21 - 12)) | ((input[28]) << 12);
    output[19] = ((input[28] & 2097151) >>> (21 - 1)) |
                 ((input[29] & 2097151) << 1) | ((input[30]) << 22);
    output[20] = ((input[30] & 2097151) >>> (21 - 11)) | ((input[31]) << 11);
  }

  private static void fastpack22(int *input, int *output) {
    output[0] = (input[0] & 4194303) | ((input[1]) << 22);
    output[1] = ((input[1] & 4194303) >>> (22 - 12)) | ((input[2]) << 12);
    output[2] = ((input[2] & 4194303) >>> (22 - 2)) |
                ((input[3] & 4194303) << 2) | ((input[4]) << 24);
    output[3] = ((input[4] & 4194303) >>> (22 - 14)) | ((input[5]) << 14);
    output[4] = ((input[5] & 4194303) >>> (22 - 4)) |
                ((input[6] & 4194303) << 4) | ((input[7]) << 26);
    output[5] = ((input[7] & 4194303) >>> (22 - 16)) | ((input[8]) << 16);
    output[6] = ((input[8] & 4194303) >>> (22 - 6)) |
                ((input[9] & 4194303) << 6) | ((input[10]) << 28);
    output[7] = ((input[10] & 4194303) >>> (22 - 18)) | ((input[11]) << 18);
    output[8] = ((input[11] & 4194303) >>> (22 - 8)) |
                ((input[12] & 4194303) << 8) | ((input[13]) << 30);
    output[9] = ((input[13] & 4194303) >>> (22 - 20)) | ((input[14]) << 20);
    output[10] = ((input[14] & 4194303) >>> (22 - 10)) | ((input[15]) << 10);
    output[11] = (input[16] & 4194303) | ((input[17]) << 22);
    output[12] = ((input[17] & 4194303) >>> (22 - 12)) | ((input[18]) << 12);
    output[13] = ((input[18] & 4194303) >>> (22 - 2)) |
                 ((input[19] & 4194303) << 2) | ((input[20]) << 24);
    output[14] = ((input[20] & 4194303) >>> (22 - 14)) | ((input[21]) << 14);
    output[15] = ((input[21] & 4194303) >>> (22 - 4)) |
                 ((input[22] & 4194303) << 4) | ((input[23]) << 26);
    output[16] = ((input[23] & 4194303) >>> (22 - 16)) | ((input[24]) << 16);
    output[17] = ((input[24] & 4194303) >>> (22 - 6)) |
                 ((input[25] & 4194303) << 6) | ((input[26]) << 28);
    output[18] = ((input[26] & 4194303) >>> (22 - 18)) | ((input[27]) << 18);
    output[19] = ((input[27] & 4194303) >>> (22 - 8)) |
                 ((input[28] & 4194303) << 8) | ((input[29]) << 30);
    output[20] = ((input[29] & 4194303) >>> (22 - 20)) | ((input[30]) << 20);
    output[21] = ((input[30] & 4194303) >>> (22 - 10)) | ((input[31]) << 10);
  }

  private static void fastpack23(int *input, int *output) {
    output[0] = (input[0] & 8388607) | ((input[1]) << 23);
    output[1] = ((input[1] & 8388607) >>> (23 - 14)) | ((input[2]) << 14);
    output[2] = ((input[2] & 8388607) >>> (23 - 5)) |
                ((input[3] & 8388607) << 5) | ((input[4]) << 28);
    output[3] = ((input[4] & 8388607) >>> (23 - 19)) | ((input[5]) << 19);
    output[4] = ((input[5] & 8388607) >>> (23 - 10)) | ((input[6]) << 10);
    output[5] = ((input[6] & 8388607) >>> (23 - 1)) |
                ((input[7] & 8388607) << 1) | ((input[8]) << 24);
    output[6] = ((input[8] & 8388607) >>> (23 - 15)) | ((input[9]) << 15);
    output[7] = ((input[9] & 8388607) >>> (23 - 6)) |
                ((input[10] & 8388607) << 6) | ((input[11]) << 29);
    output[8] = ((input[11] & 8388607) >>> (23 - 20)) | ((input[12]) << 20);
    output[9] = ((input[12] & 8388607) >>> (23 - 11)) | ((input[13]) << 11);
    output[10] = ((input[13] & 8388607) >>> (23 - 2)) |
                 ((input[14] & 8388607) << 2) | ((input[15]) << 25);
    output[11] = ((input[15] & 8388607) >>> (23 - 16)) | ((input[16]) << 16);
    output[12] = ((input[16] & 8388607) >>> (23 - 7)) |
                 ((input[17] & 8388607) << 7) | ((input[18]) << 30);
    output[13] = ((input[18] & 8388607) >>> (23 - 21)) | ((input[19]) << 21);
    output[14] = ((input[19] & 8388607) >>> (23 - 12)) | ((input[20]) << 12);
    output[15] = ((input[20] & 8388607) >>> (23 - 3)) |
                 ((input[21] & 8388607) << 3) | ((input[22]) << 26);
    output[16] = ((input[22] & 8388607) >>> (23 - 17)) | ((input[23]) << 17);
    output[17] = ((input[23] & 8388607) >>> (23 - 8)) |
                 ((input[24] & 8388607) << 8) | ((input[25]) << 31);
    output[18] = ((input[25] & 8388607) >>> (23 - 22)) | ((input[26]) << 22);
    output[19] = ((input[26] & 8388607) >>> (23 - 13)) | ((input[27]) << 13);
    output[20] = ((input[27] & 8388607) >>> (23 - 4)) |
                 ((input[28] & 8388607) << 4) | ((input[29]) << 27);
    output[21] = ((input[29] & 8388607) >>> (23 - 18)) | ((input[30]) << 18);
    output[22] = ((input[30] & 8388607) >>> (23 - 9)) | ((input[31]) << 9);
  }

  private static void fastpack24(int *input, int *output) {
    output[0] = (input[0] & 16777215) | ((input[1]) << 24);
    output[1] = ((input[1] & 16777215) >>> (24 - 16)) | ((input[2]) << 16);
    output[2] = ((input[2] & 16777215) >>> (24 - 8)) | ((input[3]) << 8);
    output[3] = (input[4] & 16777215) | ((input[5]) << 24);
    output[4] = ((input[5] & 16777215) >>> (24 - 16)) | ((input[6]) << 16);
    output[5] = ((input[6] & 16777215) >>> (24 - 8)) | ((input[7]) << 8);
    output[6] = (input[8] & 16777215) | ((input[9]) << 24);
    output[7] = ((input[9] & 16777215) >>> (24 - 16)) | ((input[10]) << 16);
    output[8] = ((input[10] & 16777215) >>> (24 - 8)) | ((input[11]) << 8);
    output[9] = (input[12] & 16777215) | ((input[13]) << 24);
    output[10] = ((input[13] & 16777215) >>> (24 - 16)) | ((input[14]) << 16);
    output[11] = ((input[14] & 16777215) >>> (24 - 8)) | ((input[15]) << 8);
    output[12] = (input[16] & 16777215) | ((input[17]) << 24);
    output[13] = ((input[17] & 16777215) >>> (24 - 16)) | ((input[18]) << 16);
    output[14] = ((input[18] & 16777215) >>> (24 - 8)) | ((input[19]) << 8);
    output[15] = (input[20] & 16777215) | ((input[21]) << 24);
    output[16] = ((input[21] & 16777215) >>> (24 - 16)) | ((input[22]) << 16);
    output[17] = ((input[22] & 16777215) >>> (24 - 8)) | ((input[23]) << 8);
    output[18] = (input[24] & 16777215) | ((input[25]) << 24);
    output[19] = ((input[25] & 16777215) >>> (24 - 16)) | ((input[26]) << 16);
    output[20] = ((input[26] & 16777215) >>> (24 - 8)) | ((input[27]) << 8);
    output[21] = (input[28] & 16777215) | ((input[29]) << 24);
    output[22] = ((input[29] & 16777215) >>> (24 - 16)) | ((input[30]) << 16);
    output[23] = ((input[30] & 16777215) >>> (24 - 8)) | ((input[31]) << 8);
  }

  private static void fastpack25(int *input, int *output) {
    output[0] = (input[0] & 33554431) | ((input[1]) << 25);
    output[1] = ((input[1] & 33554431) >>> (25 - 18)) | ((input[2]) << 18);
    output[2] = ((input[2] & 33554431) >>> (25 - 11)) | ((input[3]) << 11);
    output[3] = ((input[3] & 33554431) >>> (25 - 4)) |
                ((input[4] & 33554431) << 4) | ((input[5]) << 29);
    output[4] = ((input[5] & 33554431) >>> (25 - 22)) | ((input[6]) << 22);
    output[5] = ((input[6] & 33554431) >>> (25 - 15)) | ((input[7]) << 15);
    output[6] = ((input[7] & 33554431) >>> (25 - 8)) | ((input[8]) << 8);
    output[7] = ((input[8] & 33554431) >>> (25 - 1)) |
                ((input[9] & 33554431) << 1) | ((input[10]) << 26);
    output[8] = ((input[10] & 33554431) >>> (25 - 19)) | ((input[11]) << 19);
    output[9] = ((input[11] & 33554431) >>> (25 - 12)) | ((input[12]) << 12);
    output[10] = ((input[12] & 33554431) >>> (25 - 5)) |
                 ((input[13] & 33554431) << 5) | ((input[14]) << 30);
    output[11] = ((input[14] & 33554431) >>> (25 - 23)) | ((input[15]) << 23);
    output[12] = ((input[15] & 33554431) >>> (25 - 16)) | ((input[16]) << 16);
    output[13] = ((input[16] & 33554431) >>> (25 - 9)) | ((input[17]) << 9);
    output[14] = ((input[17] & 33554431) >>> (25 - 2)) |
                 ((input[18] & 33554431) << 2) | ((input[19]) << 27);
    output[15] = ((input[19] & 33554431) >>> (25 - 20)) | ((input[20]) << 20);
    output[16] = ((input[20] & 33554431) >>> (25 - 13)) | ((input[21]) << 13);
    output[17] = ((input[21] & 33554431) >>> (25 - 6)) |
                 ((input[22] & 33554431) << 6) | ((input[23]) << 31);
    output[18] = ((input[23] & 33554431) >>> (25 - 24)) | ((input[24]) << 24);
    output[19] = ((input[24] & 33554431) >>> (25 - 17)) | ((input[25]) << 17);
    output[20] = ((input[25] & 33554431) >>> (25 - 10)) | ((input[26]) << 10);
    output[21] = ((input[26] & 33554431) >>> (25 - 3)) |
                 ((input[27] & 33554431) << 3) | ((input[28]) << 28);
    output[22] = ((input[28] & 33554431) >>> (25 - 21)) | ((input[29]) << 21);
    output[23] = ((input[29] & 33554431) >>> (25 - 14)) | ((input[30]) << 14);
    output[24] = ((input[30] & 33554431) >>> (25 - 7)) | ((input[31]) << 7);
  }

  private static void fastpack26(int *input, int *output) {
    output[0] = (input[0] & 67108863) | ((input[1]) << 26);
    output[1] = ((input[1] & 67108863) >>> (26 - 20)) | ((input[2]) << 20);
    output[2] = ((input[2] & 67108863) >>> (26 - 14)) | ((input[3]) << 14);
    output[3] = ((input[3] & 67108863) >>> (26 - 8)) | ((input[4]) << 8);
    output[4] = ((input[4] & 67108863) >>> (26 - 2)) |
                ((input[5] & 67108863) << 2) | ((input[6]) << 28);
    output[5] = ((input[6] & 67108863) >>> (26 - 22)) | ((input[7]) << 22);
    output[6] = ((input[7] & 67108863) >>> (26 - 16)) | ((input[8]) << 16);
    output[7] = ((input[8] & 67108863) >>> (26 - 10)) | ((input[9]) << 10);
    output[8] = ((input[9] & 67108863) >>> (26 - 4)) |
                ((input[10] & 67108863) << 4) | ((input[11]) << 30);
    output[9] = ((input[11] & 67108863) >>> (26 - 24)) | ((input[12]) << 24);
    output[10] = ((input[12] & 67108863) >>> (26 - 18)) | ((input[13]) << 18);
    output[11] = ((input[13] & 67108863) >>> (26 - 12)) | ((input[14]) << 12);
    output[12] = ((input[14] & 67108863) >>> (26 - 6)) | ((input[15]) << 6);
    output[13] = (input[16] & 67108863) | ((input[17]) << 26);
    output[14] = ((input[17] & 67108863) >>> (26 - 20)) | ((input[18]) << 20);
    output[15] = ((input[18] & 67108863) >>> (26 - 14)) | ((input[19]) << 14);
    output[16] = ((input[19] & 67108863) >>> (26 - 8)) | ((input[20]) << 8);
    output[17] = ((input[20] & 67108863) >>> (26 - 2)) |
                 ((input[21] & 67108863) << 2) | ((input[22]) << 28);
    output[18] = ((input[22] & 67108863) >>> (26 - 22)) | ((input[23]) << 22);
    output[19] = ((input[23] & 67108863) >>> (26 - 16)) | ((input[24]) << 16);
    output[20] = ((input[24] & 67108863) >>> (26 - 10)) | ((input[25]) << 10);
    output[21] = ((input[25] & 67108863) >>> (26 - 4)) |
                 ((input[26] & 67108863) << 4) | ((input[27]) << 30);
    output[22] = ((input[27] & 67108863) >>> (26 - 24)) | ((input[28]) << 24);
    output[23] = ((input[28] & 67108863) >>> (26 - 18)) | ((input[29]) << 18);
    output[24] = ((input[29] & 67108863) >>> (26 - 12)) | ((input[30]) << 12);
    output[25] = ((input[30] & 67108863) >>> (26 - 6)) | ((input[31]) << 6);
  }

  private static void fastpack27(int *input, int *output) {
    output[0] = (input[0] & 134217727) | ((input[1]) << 27);
    output[1] = ((input[1] & 134217727) >>> (27 - 22)) | ((input[2]) << 22);
    output[2] = ((input[2] & 134217727) >>> (27 - 17)) | ((input[3]) << 17);
    output[3] = ((input[3] & 134217727) >>> (27 - 12)) | ((input[4]) << 12);
    output[4] = ((input[4] & 134217727) >>> (27 - 7)) | ((input[5]) << 7);
    output[5] = ((input[5] & 134217727) >>> (27 - 2)) |
                ((input[6] & 134217727) << 2) | ((input[7]) << 29);
    output[6] = ((input[7] & 134217727) >>> (27 - 24)) | ((input[8]) << 24);
    output[7] = ((input[8] & 134217727) >>> (27 - 19)) | ((input[9]) << 19);
    output[8] = ((input[9] & 134217727) >>> (27 - 14)) | ((input[10]) << 14);
    output[9] = ((input[10] & 134217727) >>> (27 - 9)) | ((input[11]) << 9);
    output[10] = ((input[11] & 134217727) >>> (27 - 4)) |
                 ((input[12] & 134217727) << 4) | ((input[13]) << 31);
    output[11] = ((input[13] & 134217727) >>> (27 - 26)) | ((input[14]) << 26);
    output[12] = ((input[14] & 134217727) >>> (27 - 21)) | ((input[15]) << 21);
    output[13] = ((input[15] & 134217727) >>> (27 - 16)) | ((input[16]) << 16);
    output[14] = ((input[16] & 134217727) >>> (27 - 11)) | ((input[17]) << 11);
    output[15] = ((input[17] & 134217727) >>> (27 - 6)) | ((input[18]) << 6);
    output[16] = ((input[18] & 134217727) >>> (27 - 1)) |
                 ((input[19] & 134217727) << 1) | ((input[20]) << 28);
    output[17] = ((input[20] & 134217727) >>> (27 - 23)) | ((input[21]) << 23);
    output[18] = ((input[21] & 134217727) >>> (27 - 18)) | ((input[22]) << 18);
    output[19] = ((input[22] & 134217727) >>> (27 - 13)) | ((input[23]) << 13);
    output[20] = ((input[23] & 134217727) >>> (27 - 8)) | ((input[24]) << 8);
    output[21] = ((input[24] & 134217727) >>> (27 - 3)) |
                 ((input[25] & 134217727) << 3) | ((input[26]) << 30);
    output[22] = ((input[26] & 134217727) >>> (27 - 25)) | ((input[27]) << 25);
    output[23] = ((input[27] & 134217727) >>> (27 - 20)) | ((input[28]) << 20);
    output[24] = ((input[28] & 134217727) >>> (27 - 15)) | ((input[29]) << 15);
    output[25] = ((input[29] & 134217727) >>> (27 - 10)) | ((input[30]) << 10);
    output[26] = ((input[30] & 134217727) >>> (27 - 5)) | ((input[31]) << 5);
  }

  private static void fastpack28(int *input, int *output) {
    output[0] = (input[0] & 268435455) | ((input[1]) << 28);
    output[1] = ((input[1] & 268435455) >>> (28 - 24)) | ((input[2]) << 24);
    output[2] = ((input[2] & 268435455) >>> (28 - 20)) | ((input[3]) << 20);
    output[3] = ((input[3] & 268435455) >>> (28 - 16)) | ((input[4]) << 16);
    output[4] = ((input[4] & 268435455) >>> (28 - 12)) | ((input[5]) << 12);
    output[5] = ((input[5] & 268435455) >>> (28 - 8)) | ((input[6]) << 8);
    output[6] = ((input[6] & 268435455) >>> (28 - 4)) | ((input[7]) << 4);
    output[7] = (input[8] & 268435455) | ((input[9]) << 28);
    output[8] = ((input[9] & 268435455) >>> (28 - 24)) | ((input[10]) << 24);
    output[9] = ((input[10] & 268435455) >>> (28 - 20)) | ((input[11]) << 20);
    output[10] = ((input[11] & 268435455) >>> (28 - 16)) | ((input[12]) << 16);
    output[11] = ((input[12] & 268435455) >>> (28 - 12)) | ((input[13]) << 12);
    output[12] = ((input[13] & 268435455) >>> (28 - 8)) | ((input[14]) << 8);
    output[13] = ((input[14] & 268435455) >>> (28 - 4)) | ((input[15]) << 4);
    output[14] = (input[16] & 268435455) | ((input[17]) << 28);
    output[15] = ((input[17] & 268435455) >>> (28 - 24)) | ((input[18]) << 24);
    output[16] = ((input[18] & 268435455) >>> (28 - 20)) | ((input[19]) << 20);
    output[17] = ((input[19] & 268435455) >>> (28 - 16)) | ((input[20]) << 16);
    output[18] = ((input[20] & 268435455) >>> (28 - 12)) | ((input[21]) << 12);
    output[19] = ((input[21] & 268435455) >>> (28 - 8)) | ((input[22]) << 8);
    output[20] = ((input[22] & 268435455) >>> (28 - 4)) | ((input[23]) << 4);
    output[21] = (input[24] & 268435455) | ((input[25]) << 28);
    output[22] = ((input[25] & 268435455) >>> (28 - 24)) | ((input[26]) << 24);
    output[23] = ((input[26] & 268435455) >>> (28 - 20)) | ((input[27]) << 20);
    output[24] = ((input[27] & 268435455) >>> (28 - 16)) | ((input[28]) << 16);
    output[25] = ((input[28] & 268435455) >>> (28 - 12)) | ((input[29]) << 12);
    output[26] = ((input[29] & 268435455) >>> (28 - 8)) | ((input[30]) << 8);
    output[27] = ((input[30] & 268435455) >>> (28 - 4)) | ((input[31]) << 4);
  }

  private static void fastpack29(int *input, int *output) {
    output[0] = (input[0] & 536870911) | ((input[1]) << 29);
    output[1] = ((input[1] & 536870911) >>> (29 - 26)) | ((input[2]) << 26);
    output[2] = ((input[2] & 536870911) >>> (29 - 23)) | ((input[3]) << 23);
    output[3] = ((input[3] & 536870911) >>> (29 - 20)) | ((input[4]) << 20);
    output[4] = ((input[4] & 536870911) >>> (29 - 17)) | ((input[5]) << 17);
    output[5] = ((input[5] & 536870911) >>> (29 - 14)) | ((input[6]) << 14);
    output[6] = ((input[6] & 536870911) >>> (29 - 11)) | ((input[7]) << 11);
    output[7] = ((input[7] & 536870911) >>> (29 - 8)) | ((input[8]) << 8);
    output[8] = ((input[8] & 536870911) >>> (29 - 5)) | ((input[9]) << 5);
    output[9] = ((input[9] & 536870911) >>> (29 - 2)) |
                ((input[10] & 536870911) << 2) | ((input[11]) << 31);
    output[10] = ((input[11] & 536870911) >>> (29 - 28)) | ((input[12]) << 28);
    output[11] = ((input[12] & 536870911) >>> (29 - 25)) | ((input[13]) << 25);
    output[12] = ((input[13] & 536870911) >>> (29 - 22)) | ((input[14]) << 22);
    output[13] = ((input[14] & 536870911) >>> (29 - 19)) | ((input[15]) << 19);
    output[14] = ((input[15] & 536870911) >>> (29 - 16)) | ((input[16]) << 16);
    output[15] = ((input[16] & 536870911) >>> (29 - 13)) | ((input[17]) << 13);
    output[16] = ((input[17] & 536870911) >>> (29 - 10)) | ((input[18]) << 10);
    output[17] = ((input[18] & 536870911) >>> (29 - 7)) | ((input[19]) << 7);
    output[18] = ((input[19] & 536870911) >>> (29 - 4)) | ((input[20]) << 4);
    output[19] = ((input[20] & 536870911) >>> (29 - 1)) |
                 ((input[21] & 536870911) << 1) | ((input[22]) << 30);
    output[20] = ((input[22] & 536870911) >>> (29 - 27)) | ((input[23]) << 27);
    output[21] = ((input[23] & 536870911) >>> (29 - 24)) | ((input[24]) << 24);
    output[22] = ((input[24] & 536870911) >>> (29 - 21)) | ((input[25]) << 21);
    output[23] = ((input[25] & 536870911) >>> (29 - 18)) | ((input[26]) << 18);
    output[24] = ((input[26] & 536870911) >>> (29 - 15)) | ((input[27]) << 15);
    output[25] = ((input[27] & 536870911) >>> (29 - 12)) | ((input[28]) << 12);
    output[26] = ((input[28] & 536870911) >>> (29 - 9)) | ((input[29]) << 9);
    output[27] = ((input[29] & 536870911) >>> (29 - 6)) | ((input[30]) << 6);
    output[28] = ((input[30] & 536870911) >>> (29 - 3)) | ((input[31]) << 3);
  }

  private static void fastpack3(int *input, int *output) {
    output[0] = (input[0] & 7) | ((input[1] & 7) << 3) | ((input[2] & 7) << 6) |
                ((input[3] & 7) << 9) | ((input[4] & 7) << 12) |
                ((input[5] & 7) << 15) | ((input[6] & 7) << 18) |
                ((input[7] & 7) << 21) | ((input[8] & 7) << 24) |
                ((input[9] & 7) << 27) | ((input[10]) << 30);
    output[1] = ((input[10] & 7) >>> (3 - 1)) | ((input[11] & 7) << 1) |
                ((input[12] & 7) << 4) | ((input[13] & 7) << 7) |
                ((input[14] & 7) << 10) | ((input[15] & 7) << 13) |
                ((input[16] & 7) << 16) | ((input[17] & 7) << 19) |
                ((input[18] & 7) << 22) | ((input[19] & 7) << 25) |
                ((input[20] & 7) << 28) | ((input[21]) << 31);
    output[2] = ((input[21] & 7) >>> (3 - 2)) | ((input[22] & 7) << 2) |
                ((input[23] & 7) << 5) | ((input[24] & 7) << 8) |
                ((input[25] & 7) << 11) | ((input[26] & 7) << 14) |
                ((input[27] & 7) << 17) | ((input[28] & 7) << 20) |
                ((input[29] & 7) << 23) | ((input[30] & 7) << 26) |
                ((input[31]) << 29);
  }

  private static void fastpack30(int *input, int *output) {
    output[0] = (input[0] & 1073741823) | ((input[1]) << 30);
    output[1] = ((input[1] & 1073741823) >>> (30 - 28)) | ((input[2]) << 28);
    output[2] = ((input[2] & 1073741823) >>> (30 - 26)) | ((input[3]) << 26);
    output[3] = ((input[3] & 1073741823) >>> (30 - 24)) | ((input[4]) << 24);
    output[4] = ((input[4] & 1073741823) >>> (30 - 22)) | ((input[5]) << 22);
    output[5] = ((input[5] & 1073741823) >>> (30 - 20)) | ((input[6]) << 20);
    output[6] = ((input[6] & 1073741823) >>> (30 - 18)) | ((input[7]) << 18);
    output[7] = ((input[7] & 1073741823) >>> (30 - 16)) | ((input[8]) << 16);
    output[8] = ((input[8] & 1073741823) >>> (30 - 14)) | ((input[9]) << 14);
    output[9] = ((input[9] & 1073741823) >>> (30 - 12)) | ((input[10]) << 12);
    output[10] = ((input[10] & 1073741823) >>> (30 - 10)) | ((input[11]) << 10);
    output[11] = ((input[11] & 1073741823) >>> (30 - 8)) | ((input[12]) << 8);
    output[12] = ((input[12] & 1073741823) >>> (30 - 6)) | ((input[13]) << 6);
    output[13] = ((input[13] & 1073741823) >>> (30 - 4)) | ((input[14]) << 4);
    output[14] = ((input[14] & 1073741823) >>> (30 - 2)) | ((input[15]) << 2);
    output[15] = (input[16] & 1073741823) | ((input[17]) << 30);
    output[16] = ((input[17] & 1073741823) >>> (30 - 28)) | ((input[18]) << 28);
    output[17] = ((input[18] & 1073741823) >>> (30 - 26)) | ((input[19]) << 26);
    output[18] = ((input[19] & 1073741823) >>> (30 - 24)) | ((input[20]) << 24);
    output[19] = ((input[20] & 1073741823) >>> (30 - 22)) | ((input[21]) << 22);
    output[20] = ((input[21] & 1073741823) >>> (30 - 20)) | ((input[22]) << 20);
    output[21] = ((input[22] & 1073741823) >>> (30 - 18)) | ((input[23]) << 18);
    output[22] = ((input[23] & 1073741823) >>> (30 - 16)) | ((input[24]) << 16);
    output[23] = ((input[24] & 1073741823) >>> (30 - 14)) | ((input[25]) << 14);
    output[24] = ((input[25] & 1073741823) >>> (30 - 12)) | ((input[26]) << 12);
    output[25] = ((input[26] & 1073741823) >>> (30 - 10)) | ((input[27]) << 10);
    output[26] = ((input[27] & 1073741823) >>> (30 - 8)) | ((input[28]) << 8);
    output[27] = ((input[28] & 1073741823) >>> (30 - 6)) | ((input[29]) << 6);
    output[28] = ((input[29] & 1073741823) >>> (30 - 4)) | ((input[30]) << 4);
    output[29] = ((input[30] & 1073741823) >>> (30 - 2)) | ((input[31]) << 2);
  }

  private static void fastpack31(int *input, int *output) {
    output[0] = (input[0] & 2147483647) | ((input[1]) << 31);
    output[1] = ((input[1] & 2147483647) >>> (31 - 30)) | ((input[2]) << 30);
    output[2] = ((input[2] & 2147483647) >>> (31 - 29)) | ((input[3]) << 29);
    output[3] = ((input[3] & 2147483647) >>> (31 - 28)) | ((input[4]) << 28);
    output[4] = ((input[4] & 2147483647) >>> (31 - 27)) | ((input[5]) << 27);
    output[5] = ((input[5] & 2147483647) >>> (31 - 26)) | ((input[6]) << 26);
    output[6] = ((input[6] & 2147483647) >>> (31 - 25)) | ((input[7]) << 25);
    output[7] = ((input[7] & 2147483647) >>> (31 - 24)) | ((input[8]) << 24);
    output[8] = ((input[8] & 2147483647) >>> (31 - 23)) | ((input[9]) << 23);
    output[9] = ((input[9] & 2147483647) >>> (31 - 22)) | ((input[10]) << 22);
    output[10] = ((input[10] & 2147483647) >>> (31 - 21)) | ((input[11]) << 21);
    output[11] = ((input[11] & 2147483647) >>> (31 - 20)) | ((input[12]) << 20);
    output[12] = ((input[12] & 2147483647) >>> (31 - 19)) | ((input[13]) << 19);
    output[13] = ((input[13] & 2147483647) >>> (31 - 18)) | ((input[14]) << 18);
    output[14] = ((input[14] & 2147483647) >>> (31 - 17)) | ((input[15]) << 17);
    output[15] = ((input[15] & 2147483647) >>> (31 - 16)) | ((input[16]) << 16);
    output[16] = ((input[16] & 2147483647) >>> (31 - 15)) | ((input[17]) << 15);
    output[17] = ((input[17] & 2147483647) >>> (31 - 14)) | ((input[18]) << 14);
    output[18] = ((input[18] & 2147483647) >>> (31 - 13)) | ((input[19]) << 13);
    output[19] = ((input[19] & 2147483647) >>> (31 - 12)) | ((input[20]) << 12);
    output[20] = ((input[20] & 2147483647) >>> (31 - 11)) | ((input[21]) << 11);
    output[21] = ((input[21] & 2147483647) >>> (31 - 10)) | ((input[22]) << 10);
    output[22] = ((input[22] & 2147483647) >>> (31 - 9)) | ((input[23]) << 9);
    output[23] = ((input[23] & 2147483647) >>> (31 - 8)) | ((input[24]) << 8);
    output[24] = ((input[24] & 2147483647) >>> (31 - 7)) | ((input[25]) << 7);
    output[25] = ((input[25] & 2147483647) >>> (31 - 6)) | ((input[26]) << 6);
    output[26] = ((input[26] & 2147483647) >>> (31 - 5)) | ((input[27]) << 5);
    output[27] = ((input[27] & 2147483647) >>> (31 - 4)) | ((input[28]) << 4);
    output[28] = ((input[28] & 2147483647) >>> (31 - 3)) | ((input[29]) << 3);
    output[29] = ((input[29] & 2147483647) >>> (31 - 2)) | ((input[30]) << 2);
    output[30] = ((input[30] & 2147483647) >>> (31 - 1)) | ((input[31]) << 1);
  }

  private static void fastpack32(int *input, int *output) {
    Unsafe.CopyBlock(output, input, 32);
  }

  private static void fastpack4(int *input, int *output) {
    output[0] = (input[0] & 15) | ((input[1] & 15) << 4) |
                ((input[2] & 15) << 8) | ((input[3] & 15) << 12) |
                ((input[4] & 15) << 16) | ((input[5] & 15) << 20) |
                ((input[6] & 15) << 24) | ((input[7]) << 28);
    output[1] = (input[8] & 15) | ((input[9] & 15) << 4) |
                ((input[10] & 15) << 8) | ((input[11] & 15) << 12) |
                ((input[12] & 15) << 16) | ((input[13] & 15) << 20) |
                ((input[14] & 15) << 24) | ((input[15]) << 28);
    output[2] = (input[16] & 15) | ((input[17] & 15) << 4) |
                ((input[18] & 15) << 8) | ((input[19] & 15) << 12) |
                ((input[20] & 15) << 16) | ((input[21] & 15) << 20) |
                ((input[22] & 15) << 24) | ((input[23]) << 28);
    output[3] = (input[24] & 15) | ((input[25] & 15) << 4) |
                ((input[26] & 15) << 8) | ((input[27] & 15) << 12) |
                ((input[28] & 15) << 16) | ((input[29] & 15) << 20) |
                ((input[30] & 15) << 24) | ((input[31]) << 28);
  }

  private static void fastpack5(int *input, int *output) {
    output[0] = (input[0] & 31) | ((input[1] & 31) << 5) |
                ((input[2] & 31) << 10) | ((input[3] & 31) << 15) |
                ((input[4] & 31) << 20) | ((input[5] & 31) << 25) |
                ((input[6]) << 30);
    output[1] = ((input[6] & 31) >>> (5 - 3)) | ((input[7] & 31) << 3) |
                ((input[8] & 31) << 8) | ((input[9] & 31) << 13) |
                ((input[10] & 31) << 18) | ((input[11] & 31) << 23) |
                ((input[12]) << 28);
    output[2] = ((input[12] & 31) >>> (5 - 1)) | ((input[13] & 31) << 1) |
                ((input[14] & 31) << 6) | ((input[15] & 31) << 11) |
                ((input[16] & 31) << 16) | ((input[17] & 31) << 21) |
                ((input[18] & 31) << 26) | ((input[19]) << 31);
    output[3] = ((input[19] & 31) >>> (5 - 4)) | ((input[20] & 31) << 4) |
                ((input[21] & 31) << 9) | ((input[22] & 31) << 14) |
                ((input[23] & 31) << 19) | ((input[24] & 31) << 24) |
                ((input[25]) << 29);
    output[4] = ((input[25] & 31) >>> (5 - 2)) | ((input[26] & 31) << 2) |
                ((input[27] & 31) << 7) | ((input[28] & 31) << 12) |
                ((input[29] & 31) << 17) | ((input[30] & 31) << 22) |
                ((input[31]) << 27);
  }

  private static void fastpack6(int *input, int *output) {
    output[0] = (input[0] & 63) | ((input[1] & 63) << 6) |
                ((input[2] & 63) << 12) | ((input[3] & 63) << 18) |
                ((input[4] & 63) << 24) | ((input[5]) << 30);
    output[1] = ((input[5] & 63) >>> (6 - 4)) | ((input[6] & 63) << 4) |
                ((input[7] & 63) << 10) | ((input[8] & 63) << 16) |
                ((input[9] & 63) << 22) | ((input[10]) << 28);
    output[2] = ((input[10] & 63) >>> (6 - 2)) | ((input[11] & 63) << 2) |
                ((input[12] & 63) << 8) | ((input[13] & 63) << 14) |
                ((input[14] & 63) << 20) | ((input[15]) << 26);
    output[3] = (input[16] & 63) | ((input[17] & 63) << 6) |
                ((input[18] & 63) << 12) | ((input[19] & 63) << 18) |
                ((input[20] & 63) << 24) | ((input[21]) << 30);
    output[4] = ((input[21] & 63) >>> (6 - 4)) | ((input[22] & 63) << 4) |
                ((input[23] & 63) << 10) | ((input[24] & 63) << 16) |
                ((input[25] & 63) << 22) | ((input[26]) << 28);
    output[5] = ((input[26] & 63) >>> (6 - 2)) | ((input[27] & 63) << 2) |
                ((input[28] & 63) << 8) | ((input[29] & 63) << 14) |
                ((input[30] & 63) << 20) | ((input[31]) << 26);
  }

  private static void fastpack7(int *input, int *output) {
    output[0] = (input[0] & 127) | ((input[1] & 127) << 7) |
                ((input[2] & 127) << 14) | ((input[3] & 127) << 21) |
                ((input[4]) << 28);
    output[1] = ((input[4] & 127) >>> (7 - 3)) | ((input[5] & 127) << 3) |
                ((input[6] & 127) << 10) | ((input[7] & 127) << 17) |
                ((input[8] & 127) << 24) | ((input[9]) << 31);
    output[2] = ((input[9] & 127) >>> (7 - 6)) | ((input[10] & 127) << 6) |
                ((input[11] & 127) << 13) | ((input[12] & 127) << 20) |
                ((input[13]) << 27);
    output[3] = ((input[13] & 127) >>> (7 - 2)) | ((input[14] & 127) << 2) |
                ((input[15] & 127) << 9) | ((input[16] & 127) << 16) |
                ((input[17] & 127) << 23) | ((input[18]) << 30);
    output[4] = ((input[18] & 127) >>> (7 - 5)) | ((input[19] & 127) << 5) |
                ((input[20] & 127) << 12) | ((input[21] & 127) << 19) |
                ((input[22]) << 26);
    output[5] = ((input[22] & 127) >>> (7 - 1)) | ((input[23] & 127) << 1) |
                ((input[24] & 127) << 8) | ((input[25] & 127) << 15) |
                ((input[26] & 127) << 22) | ((input[27]) << 29);
    output[6] = ((input[27] & 127) >>> (7 - 4)) | ((input[28] & 127) << 4) |
                ((input[29] & 127) << 11) | ((input[30] & 127) << 18) |
                ((input[31]) << 25);
  }

  private static void fastpack8(int *input, int *output) {
    output[0] = (input[0] & 255) | ((input[1] & 255) << 8) |
                ((input[2] & 255) << 16) | ((input[3]) << 24);
    output[1] = (input[4] & 255) | ((input[5] & 255) << 8) |
                ((input[6] & 255) << 16) | ((input[7]) << 24);
    output[2] = (input[8] & 255) | ((input[9] & 255) << 8) |
                ((input[10] & 255) << 16) | ((input[11]) << 24);
    output[3] = (input[12] & 255) | ((input[13] & 255) << 8) |
                ((input[14] & 255) << 16) | ((input[15]) << 24);
    output[4] = (input[16] & 255) | ((input[17] & 255) << 8) |
                ((input[18] & 255) << 16) | ((input[19]) << 24);
    output[5] = (input[20] & 255) | ((input[21] & 255) << 8) |
                ((input[22] & 255) << 16) | ((input[23]) << 24);
    output[6] = (input[24] & 255) | ((input[25] & 255) << 8) |
                ((input[26] & 255) << 16) | ((input[27]) << 24);
    output[7] = (input[28] & 255) | ((input[29] & 255) << 8) |
                ((input[30] & 255) << 16) | ((input[31]) << 24);
  }

  private static void fastpack9(int *input, int *output) {
    output[0] = (input[0] & 511) | ((input[1] & 511) << 9) |
                ((input[2] & 511) << 18) | ((input[3]) << 27);
    output[1] = ((input[3] & 511) >>> (9 - 4)) | ((input[4] & 511) << 4) |
                ((input[5] & 511) << 13) | ((input[6] & 511) << 22) |
                ((input[7]) << 31);
    output[2] = ((input[7] & 511) >>> (9 - 8)) | ((input[8] & 511) << 8) |
                ((input[9] & 511) << 17) | ((input[10]) << 26);
    output[3] = ((input[10] & 511) >>> (9 - 3)) | ((input[11] & 511) << 3) |
                ((input[12] & 511) << 12) | ((input[13] & 511) << 21) |
                ((input[14]) << 30);
    output[4] = ((input[14] & 511) >>> (9 - 7)) | ((input[15] & 511) << 7) |
                ((input[16] & 511) << 16) | ((input[17]) << 25);
    output[5] = ((input[17] & 511) >>> (9 - 2)) | ((input[18] & 511) << 2) |
                ((input[19] & 511) << 11) | ((input[20] & 511) << 20) |
                ((input[21]) << 29);
    output[6] = ((input[21] & 511) >>> (9 - 6)) | ((input[22] & 511) << 6) |
                ((input[23] & 511) << 15) | ((input[24]) << 24);
    output[7] = ((input[24] & 511) >>> (9 - 1)) | ((input[25] & 511) << 1) |
                ((input[26] & 511) << 10) | ((input[27] & 511) << 19) |
                ((input[28]) << 28);
    output[8] = ((input[28] & 511) >>> (9 - 5)) | ((input[29] & 511) << 5) |
                ((input[30] & 511) << 14) | ((input[31]) << 23);
  }

  public static void Pack32(int *input, int *output, int bit) {
    switch (bit) {
    case 0:
      fastpackwithoutmask0(input, output);
      break;
    case 1:
      fastpackwithoutmask1(input, output);
      break;
    case 2:
      fastpackwithoutmask2(input, output);
      break;
    case 3:
      fastpackwithoutmask3(input, output);
      break;
    case 4:
      fastpackwithoutmask4(input, output);
      break;
    case 5:
      fastpackwithoutmask5(input, output);
      break;
    case 6:
      fastpackwithoutmask6(input, output);
      break;
    case 7:
      fastpackwithoutmask7(input, output);
      break;
    case 8:
      fastpackwithoutmask8(input, output);
      break;
    case 9:
      fastpackwithoutmask9(input, output);
      break;
    case 10:
      fastpackwithoutmask10(input, output);
      break;
    case 11:
      fastpackwithoutmask11(input, output);
      break;
    case 12:
      fastpackwithoutmask12(input, output);
      break;
    case 13:
      fastpackwithoutmask13(input, output);
      break;
    case 14:
      fastpackwithoutmask14(input, output);
      break;
    case 15:
      fastpackwithoutmask15(input, output);
      break;
    case 16:
      fastpackwithoutmask16(input, output);
      break;
    case 17:
      fastpackwithoutmask17(input, output);
      break;
    case 18:
      fastpackwithoutmask18(input, output);
      break;
    case 19:
      fastpackwithoutmask19(input, output);
      break;
    case 20:
      fastpackwithoutmask20(input, output);
      break;
    case 21:
      fastpackwithoutmask21(input, output);
      break;
    case 22:
      fastpackwithoutmask22(input, output);
      break;
    case 23:
      fastpackwithoutmask23(input, output);
      break;
    case 24:
      fastpackwithoutmask24(input, output);
      break;
    case 25:
      fastpackwithoutmask25(input, output);
      break;
    case 26:
      fastpackwithoutmask26(input, output);
      break;
    case 27:
      fastpackwithoutmask27(input, output);
      break;
    case 28:
      fastpackwithoutmask28(input, output);
      break;
    case 29:
      fastpackwithoutmask29(input, output);
      break;
    case 30:
      fastpackwithoutmask30(input, output);
      break;
    case 31:
      fastpackwithoutmask31(input, output);
      break;
    case 32:
      fastpackwithoutmask32(input, output);
      break;
    default:
      throw new NotSupportedException("Unsupported bit width.");
    }
  }

  private static void fastpackwithoutmask0(int *input, int *output) {
    // nothing
  }

  private static void fastpackwithoutmask1(int *input, int *output) {
    output[0] =
        input[0] | ((input[1]) << 1) | ((input[2]) << 2) | ((input[3]) << 3) |
        ((input[4]) << 4) | ((input[5]) << 5) | ((input[6]) << 6) |
        ((input[7]) << 7) | ((input[8]) << 8) | ((input[9]) << 9) |
        ((input[10]) << 10) | ((input[11]) << 11) | ((input[12]) << 12) |
        ((input[13]) << 13) | ((input[14]) << 14) | ((input[15]) << 15) |
        ((input[16]) << 16) | ((input[17]) << 17) | ((input[18]) << 18) |
        ((input[19]) << 19) | ((input[20]) << 20) | ((input[21]) << 21) |
        ((input[22]) << 22) | ((input[23]) << 23) | ((input[24]) << 24) |
        ((input[25]) << 25) | ((input[26]) << 26) | ((input[27]) << 27) |
        ((input[28]) << 28) | ((input[29]) << 29) | ((input[30]) << 30) |
        ((input[31]) << 31);
  }

  private static void fastpackwithoutmask10(int *input, int *output) {
    output[0] =
        input[0] | ((input[1]) << 10) | ((input[2]) << 20) | ((input[3]) << 30);
    output[1] = ((input[3]) >>> (10 - 8)) | ((input[4]) << 8) |
                ((input[5]) << 18) | ((input[6]) << 28);
    output[2] = ((input[6]) >>> (10 - 6)) | ((input[7]) << 6) |
                ((input[8]) << 16) | ((input[9]) << 26);
    output[3] = ((input[9]) >>> (10 - 4)) | ((input[10]) << 4) |
                ((input[11]) << 14) | ((input[12]) << 24);
    output[4] = ((input[12]) >>> (10 - 2)) | ((input[13]) << 2) |
                ((input[14]) << 12) | ((input[15]) << 22);
    output[5] = input[16] | ((input[17]) << 10) | ((input[18]) << 20) |
                ((input[19]) << 30);
    output[6] = ((input[19]) >>> (10 - 8)) | ((input[20]) << 8) |
                ((input[21]) << 18) | ((input[22]) << 28);
    output[7] = ((input[22]) >>> (10 - 6)) | ((input[23]) << 6) |
                ((input[24]) << 16) | ((input[25]) << 26);
    output[8] = ((input[25]) >>> (10 - 4)) | ((input[26]) << 4) |
                ((input[27]) << 14) | ((input[28]) << 24);
    output[9] = ((input[28]) >>> (10 - 2)) | ((input[29]) << 2) |
                ((input[30]) << 12) | ((input[31]) << 22);
  }

  private static void fastpackwithoutmask11(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 11) | ((input[2]) << 22);
    output[1] = ((input[2]) >>> (11 - 1)) | ((input[3]) << 1) |
                ((input[4]) << 12) | ((input[5]) << 23);
    output[2] = ((input[5]) >>> (11 - 2)) | ((input[6]) << 2) |
                ((input[7]) << 13) | ((input[8]) << 24);
    output[3] = ((input[8]) >>> (11 - 3)) | ((input[9]) << 3) |
                ((input[10]) << 14) | ((input[11]) << 25);
    output[4] = ((input[11]) >>> (11 - 4)) | ((input[12]) << 4) |
                ((input[13]) << 15) | ((input[14]) << 26);
    output[5] = ((input[14]) >>> (11 - 5)) | ((input[15]) << 5) |
                ((input[16]) << 16) | ((input[17]) << 27);
    output[6] = ((input[17]) >>> (11 - 6)) | ((input[18]) << 6) |
                ((input[19]) << 17) | ((input[20]) << 28);
    output[7] = ((input[20]) >>> (11 - 7)) | ((input[21]) << 7) |
                ((input[22]) << 18) | ((input[23]) << 29);
    output[8] = ((input[23]) >>> (11 - 8)) | ((input[24]) << 8) |
                ((input[25]) << 19) | ((input[26]) << 30);
    output[9] = ((input[26]) >>> (11 - 9)) | ((input[27]) << 9) |
                ((input[28]) << 20) | ((input[29]) << 31);
    output[10] =
        ((input[29]) >>> (11 - 10)) | ((input[30]) << 10) | ((input[31]) << 21);
  }

  private static void fastpackwithoutmask12(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 12) | ((input[2]) << 24);
    output[1] = ((input[2]) >>> (12 - 4)) | ((input[3]) << 4) |
                ((input[4]) << 16) | ((input[5]) << 28);
    output[2] =
        ((input[5]) >>> (12 - 8)) | ((input[6]) << 8) | ((input[7]) << 20);
    output[3] = input[8] | ((input[9]) << 12) | ((input[10]) << 24);
    output[4] = ((input[10]) >>> (12 - 4)) | ((input[11]) << 4) |
                ((input[12]) << 16) | ((input[13]) << 28);
    output[5] =
        ((input[13]) >>> (12 - 8)) | ((input[14]) << 8) | ((input[15]) << 20);
    output[6] = input[16] | ((input[17]) << 12) | ((input[18]) << 24);
    output[7] = ((input[18]) >>> (12 - 4)) | ((input[19]) << 4) |
                ((input[20]) << 16) | ((input[21]) << 28);
    output[8] =
        ((input[21]) >>> (12 - 8)) | ((input[22]) << 8) | ((input[23]) << 20);
    output[9] = input[24] | ((input[25]) << 12) | ((input[26]) << 24);
    output[10] = ((input[26]) >>> (12 - 4)) | ((input[27]) << 4) |
                 ((input[28]) << 16) | ((input[29]) << 28);
    output[11] =
        ((input[29]) >>> (12 - 8)) | ((input[30]) << 8) | ((input[31]) << 20);
  }

  private static void fastpackwithoutmask13(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 13) | ((input[2]) << 26);
    output[1] =
        ((input[2]) >>> (13 - 7)) | ((input[3]) << 7) | ((input[4]) << 20);
    output[2] = ((input[4]) >>> (13 - 1)) | ((input[5]) << 1) |
                ((input[6]) << 14) | ((input[7]) << 27);
    output[3] =
        ((input[7]) >>> (13 - 8)) | ((input[8]) << 8) | ((input[9]) << 21);
    output[4] = ((input[9]) >>> (13 - 2)) | ((input[10]) << 2) |
                ((input[11]) << 15) | ((input[12]) << 28);
    output[5] =
        ((input[12]) >>> (13 - 9)) | ((input[13]) << 9) | ((input[14]) << 22);
    output[6] = ((input[14]) >>> (13 - 3)) | ((input[15]) << 3) |
                ((input[16]) << 16) | ((input[17]) << 29);
    output[7] =
        ((input[17]) >>> (13 - 10)) | ((input[18]) << 10) | ((input[19]) << 23);
    output[8] = ((input[19]) >>> (13 - 4)) | ((input[20]) << 4) |
                ((input[21]) << 17) | ((input[22]) << 30);
    output[9] =
        ((input[22]) >>> (13 - 11)) | ((input[23]) << 11) | ((input[24]) << 24);
    output[10] = ((input[24]) >>> (13 - 5)) | ((input[25]) << 5) |
                 ((input[26]) << 18) | ((input[27]) << 31);
    output[11] =
        ((input[27]) >>> (13 - 12)) | ((input[28]) << 12) | ((input[29]) << 25);
    output[12] =
        ((input[29]) >>> (13 - 6)) | ((input[30]) << 6) | ((input[31]) << 19);
  }

  private static void fastpackwithoutmask14(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 14) | ((input[2]) << 28);
    output[1] =
        ((input[2]) >>> (14 - 10)) | ((input[3]) << 10) | ((input[4]) << 24);
    output[2] =
        ((input[4]) >>> (14 - 6)) | ((input[5]) << 6) | ((input[6]) << 20);
    output[3] = ((input[6]) >>> (14 - 2)) | ((input[7]) << 2) |
                ((input[8]) << 16) | ((input[9]) << 30);
    output[4] =
        ((input[9]) >>> (14 - 12)) | ((input[10]) << 12) | ((input[11]) << 26);
    output[5] =
        ((input[11]) >>> (14 - 8)) | ((input[12]) << 8) | ((input[13]) << 22);
    output[6] =
        ((input[13]) >>> (14 - 4)) | ((input[14]) << 4) | ((input[15]) << 18);
    output[7] = input[16] | ((input[17]) << 14) | ((input[18]) << 28);
    output[8] =
        ((input[18]) >>> (14 - 10)) | ((input[19]) << 10) | ((input[20]) << 24);
    output[9] =
        ((input[20]) >>> (14 - 6)) | ((input[21]) << 6) | ((input[22]) << 20);
    output[10] = ((input[22]) >>> (14 - 2)) | ((input[23]) << 2) |
                 ((input[24]) << 16) | ((input[25]) << 30);
    output[11] =
        ((input[25]) >>> (14 - 12)) | ((input[26]) << 12) | ((input[27]) << 26);
    output[12] =
        ((input[27]) >>> (14 - 8)) | ((input[28]) << 8) | ((input[29]) << 22);
    output[13] =
        ((input[29]) >>> (14 - 4)) | ((input[30]) << 4) | ((input[31]) << 18);
  }

  private static void fastpackwithoutmask15(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 15) | ((input[2]) << 30);
    output[1] =
        ((input[2]) >>> (15 - 13)) | ((input[3]) << 13) | ((input[4]) << 28);
    output[2] =
        ((input[4]) >>> (15 - 11)) | ((input[5]) << 11) | ((input[6]) << 26);
    output[3] =
        ((input[6]) >>> (15 - 9)) | ((input[7]) << 9) | ((input[8]) << 24);
    output[4] =
        ((input[8]) >>> (15 - 7)) | ((input[9]) << 7) | ((input[10]) << 22);
    output[5] =
        ((input[10]) >>> (15 - 5)) | ((input[11]) << 5) | ((input[12]) << 20);
    output[6] =
        ((input[12]) >>> (15 - 3)) | ((input[13]) << 3) | ((input[14]) << 18);
    output[7] = ((input[14]) >>> (15 - 1)) | ((input[15]) << 1) |
                ((input[16]) << 16) | ((input[17]) << 31);
    output[8] =
        ((input[17]) >>> (15 - 14)) | ((input[18]) << 14) | ((input[19]) << 29);
    output[9] =
        ((input[19]) >>> (15 - 12)) | ((input[20]) << 12) | ((input[21]) << 27);
    output[10] =
        ((input[21]) >>> (15 - 10)) | ((input[22]) << 10) | ((input[23]) << 25);
    output[11] =
        ((input[23]) >>> (15 - 8)) | ((input[24]) << 8) | ((input[25]) << 23);
    output[12] =
        ((input[25]) >>> (15 - 6)) | ((input[26]) << 6) | ((input[27]) << 21);
    output[13] =
        ((input[27]) >>> (15 - 4)) | ((input[28]) << 4) | ((input[29]) << 19);
    output[14] =
        ((input[29]) >>> (15 - 2)) | ((input[30]) << 2) | ((input[31]) << 17);
  }

  private static void fastpackwithoutmask16(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 16);
    output[1] = input[2] | ((input[3]) << 16);
    output[2] = input[4] | ((input[5]) << 16);
    output[3] = input[6] | ((input[7]) << 16);
    output[4] = input[8] | ((input[9]) << 16);
    output[5] = input[10] | ((input[11]) << 16);
    output[6] = input[12] | ((input[13]) << 16);
    output[7] = input[14] | ((input[15]) << 16);
    output[8] = input[16] | ((input[17]) << 16);
    output[9] = input[18] | ((input[19]) << 16);
    output[10] = input[20] | ((input[21]) << 16);
    output[11] = input[22] | ((input[23]) << 16);
    output[12] = input[24] | ((input[25]) << 16);
    output[13] = input[26] | ((input[27]) << 16);
    output[14] = input[28] | ((input[29]) << 16);
    output[15] = input[30] | ((input[31]) << 16);
  }

  private static void fastpackwithoutmask17(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 17);
    output[1] =
        ((input[1]) >>> (17 - 2)) | ((input[2]) << 2) | ((input[3]) << 19);
    output[2] =
        ((input[3]) >>> (17 - 4)) | ((input[4]) << 4) | ((input[5]) << 21);
    output[3] =
        ((input[5]) >>> (17 - 6)) | ((input[6]) << 6) | ((input[7]) << 23);
    output[4] =
        ((input[7]) >>> (17 - 8)) | ((input[8]) << 8) | ((input[9]) << 25);
    output[5] =
        ((input[9]) >>> (17 - 10)) | ((input[10]) << 10) | ((input[11]) << 27);
    output[6] =
        ((input[11]) >>> (17 - 12)) | ((input[12]) << 12) | ((input[13]) << 29);
    output[7] =
        ((input[13]) >>> (17 - 14)) | ((input[14]) << 14) | ((input[15]) << 31);
    output[8] = ((input[15]) >>> (17 - 16)) | ((input[16]) << 16);
    output[9] =
        ((input[16]) >>> (17 - 1)) | ((input[17]) << 1) | ((input[18]) << 18);
    output[10] =
        ((input[18]) >>> (17 - 3)) | ((input[19]) << 3) | ((input[20]) << 20);
    output[11] =
        ((input[20]) >>> (17 - 5)) | ((input[21]) << 5) | ((input[22]) << 22);
    output[12] =
        ((input[22]) >>> (17 - 7)) | ((input[23]) << 7) | ((input[24]) << 24);
    output[13] =
        ((input[24]) >>> (17 - 9)) | ((input[25]) << 9) | ((input[26]) << 26);
    output[14] =
        ((input[26]) >>> (17 - 11)) | ((input[27]) << 11) | ((input[28]) << 28);
    output[15] =
        ((input[28]) >>> (17 - 13)) | ((input[29]) << 13) | ((input[30]) << 30);
    output[16] = ((input[30]) >>> (17 - 15)) | ((input[31]) << 15);
  }

  private static void fastpackwithoutmask18(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 18);
    output[1] =
        ((input[1]) >>> (18 - 4)) | ((input[2]) << 4) | ((input[3]) << 22);
    output[2] =
        ((input[3]) >>> (18 - 8)) | ((input[4]) << 8) | ((input[5]) << 26);
    output[3] =
        ((input[5]) >>> (18 - 12)) | ((input[6]) << 12) | ((input[7]) << 30);
    output[4] = ((input[7]) >>> (18 - 16)) | ((input[8]) << 16);
    output[5] =
        ((input[8]) >>> (18 - 2)) | ((input[9]) << 2) | ((input[10]) << 20);
    output[6] =
        ((input[10]) >>> (18 - 6)) | ((input[11]) << 6) | ((input[12]) << 24);
    output[7] =
        ((input[12]) >>> (18 - 10)) | ((input[13]) << 10) | ((input[14]) << 28);
    output[8] = ((input[14]) >>> (18 - 14)) | ((input[15]) << 14);
    output[9] = input[16] | ((input[17]) << 18);
    output[10] =
        ((input[17]) >>> (18 - 4)) | ((input[18]) << 4) | ((input[19]) << 22);
    output[11] =
        ((input[19]) >>> (18 - 8)) | ((input[20]) << 8) | ((input[21]) << 26);
    output[12] =
        ((input[21]) >>> (18 - 12)) | ((input[22]) << 12) | ((input[23]) << 30);
    output[13] = ((input[23]) >>> (18 - 16)) | ((input[24]) << 16);
    output[14] =
        ((input[24]) >>> (18 - 2)) | ((input[25]) << 2) | ((input[26]) << 20);
    output[15] =
        ((input[26]) >>> (18 - 6)) | ((input[27]) << 6) | ((input[28]) << 24);
    output[16] =
        ((input[28]) >>> (18 - 10)) | ((input[29]) << 10) | ((input[30]) << 28);
    output[17] = ((input[30]) >>> (18 - 14)) | ((input[31]) << 14);
  }

  private static void fastpackwithoutmask19(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 19);
    output[1] =
        ((input[1]) >>> (19 - 6)) | ((input[2]) << 6) | ((input[3]) << 25);
    output[2] =
        ((input[3]) >>> (19 - 12)) | ((input[4]) << 12) | ((input[5]) << 31);
    output[3] = ((input[5]) >>> (19 - 18)) | ((input[6]) << 18);
    output[4] =
        ((input[6]) >>> (19 - 5)) | ((input[7]) << 5) | ((input[8]) << 24);
    output[5] =
        ((input[8]) >>> (19 - 11)) | ((input[9]) << 11) | ((input[10]) << 30);
    output[6] = ((input[10]) >>> (19 - 17)) | ((input[11]) << 17);
    output[7] =
        ((input[11]) >>> (19 - 4)) | ((input[12]) << 4) | ((input[13]) << 23);
    output[8] =
        ((input[13]) >>> (19 - 10)) | ((input[14]) << 10) | ((input[15]) << 29);
    output[9] = ((input[15]) >>> (19 - 16)) | ((input[16]) << 16);
    output[10] =
        ((input[16]) >>> (19 - 3)) | ((input[17]) << 3) | ((input[18]) << 22);
    output[11] =
        ((input[18]) >>> (19 - 9)) | ((input[19]) << 9) | ((input[20]) << 28);
    output[12] = ((input[20]) >>> (19 - 15)) | ((input[21]) << 15);
    output[13] =
        ((input[21]) >>> (19 - 2)) | ((input[22]) << 2) | ((input[23]) << 21);
    output[14] =
        ((input[23]) >>> (19 - 8)) | ((input[24]) << 8) | ((input[25]) << 27);
    output[15] = ((input[25]) >>> (19 - 14)) | ((input[26]) << 14);
    output[16] =
        ((input[26]) >>> (19 - 1)) | ((input[27]) << 1) | ((input[28]) << 20);
    output[17] =
        ((input[28]) >>> (19 - 7)) | ((input[29]) << 7) | ((input[30]) << 26);
    output[18] = ((input[30]) >>> (19 - 13)) | ((input[31]) << 13);
  }

  private static void fastpackwithoutmask2(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 2) | ((input[2]) << 4) |
                ((input[3]) << 6) | ((input[4]) << 8) | ((input[5]) << 10) |
                ((input[6]) << 12) | ((input[7]) << 14) | ((input[8]) << 16) |
                ((input[9]) << 18) | ((input[10]) << 20) | ((input[11]) << 22) |
                ((input[12]) << 24) | ((input[13]) << 26) |
                ((input[14]) << 28) | ((input[15]) << 30);
    output[1] = input[16] | ((input[17]) << 2) | ((input[18]) << 4) |
                ((input[19]) << 6) | ((input[20]) << 8) | ((input[21]) << 10) |
                ((input[22]) << 12) | ((input[23]) << 14) |
                ((input[24]) << 16) | ((input[25]) << 18) |
                ((input[26]) << 20) | ((input[27]) << 22) |
                ((input[28]) << 24) | ((input[29]) << 26) |
                ((input[30]) << 28) | ((input[31]) << 30);
  }

  private static void fastpackwithoutmask20(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 20);
    output[1] =
        ((input[1]) >>> (20 - 8)) | ((input[2]) << 8) | ((input[3]) << 28);
    output[2] = ((input[3]) >>> (20 - 16)) | ((input[4]) << 16);
    output[3] =
        ((input[4]) >>> (20 - 4)) | ((input[5]) << 4) | ((input[6]) << 24);
    output[4] = ((input[6]) >>> (20 - 12)) | ((input[7]) << 12);
    output[5] = input[8] | ((input[9]) << 20);
    output[6] =
        ((input[9]) >>> (20 - 8)) | ((input[10]) << 8) | ((input[11]) << 28);
    output[7] = ((input[11]) >>> (20 - 16)) | ((input[12]) << 16);
    output[8] =
        ((input[12]) >>> (20 - 4)) | ((input[13]) << 4) | ((input[14]) << 24);
    output[9] = ((input[14]) >>> (20 - 12)) | ((input[15]) << 12);
    output[10] = input[16] | ((input[17]) << 20);
    output[11] =
        ((input[17]) >>> (20 - 8)) | ((input[18]) << 8) | ((input[19]) << 28);
    output[12] = ((input[19]) >>> (20 - 16)) | ((input[20]) << 16);
    output[13] =
        ((input[20]) >>> (20 - 4)) | ((input[21]) << 4) | ((input[22]) << 24);
    output[14] = ((input[22]) >>> (20 - 12)) | ((input[23]) << 12);
    output[15] = input[24] | ((input[25]) << 20);
    output[16] =
        ((input[25]) >>> (20 - 8)) | ((input[26]) << 8) | ((input[27]) << 28);
    output[17] = ((input[27]) >>> (20 - 16)) | ((input[28]) << 16);
    output[18] =
        ((input[28]) >>> (20 - 4)) | ((input[29]) << 4) | ((input[30]) << 24);
    output[19] = ((input[30]) >>> (20 - 12)) | ((input[31]) << 12);
  }

  private static void fastpackwithoutmask21(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 21);
    output[1] =
        ((input[1]) >>> (21 - 10)) | ((input[2]) << 10) | ((input[3]) << 31);
    output[2] = ((input[3]) >>> (21 - 20)) | ((input[4]) << 20);
    output[3] =
        ((input[4]) >>> (21 - 9)) | ((input[5]) << 9) | ((input[6]) << 30);
    output[4] = ((input[6]) >>> (21 - 19)) | ((input[7]) << 19);
    output[5] =
        ((input[7]) >>> (21 - 8)) | ((input[8]) << 8) | ((input[9]) << 29);
    output[6] = ((input[9]) >>> (21 - 18)) | ((input[10]) << 18);
    output[7] =
        ((input[10]) >>> (21 - 7)) | ((input[11]) << 7) | ((input[12]) << 28);
    output[8] = ((input[12]) >>> (21 - 17)) | ((input[13]) << 17);
    output[9] =
        ((input[13]) >>> (21 - 6)) | ((input[14]) << 6) | ((input[15]) << 27);
    output[10] = ((input[15]) >>> (21 - 16)) | ((input[16]) << 16);
    output[11] =
        ((input[16]) >>> (21 - 5)) | ((input[17]) << 5) | ((input[18]) << 26);
    output[12] = ((input[18]) >>> (21 - 15)) | ((input[19]) << 15);
    output[13] =
        ((input[19]) >>> (21 - 4)) | ((input[20]) << 4) | ((input[21]) << 25);
    output[14] = ((input[21]) >>> (21 - 14)) | ((input[22]) << 14);
    output[15] =
        ((input[22]) >>> (21 - 3)) | ((input[23]) << 3) | ((input[24]) << 24);
    output[16] = ((input[24]) >>> (21 - 13)) | ((input[25]) << 13);
    output[17] =
        ((input[25]) >>> (21 - 2)) | ((input[26]) << 2) | ((input[27]) << 23);
    output[18] = ((input[27]) >>> (21 - 12)) | ((input[28]) << 12);
    output[19] =
        ((input[28]) >>> (21 - 1)) | ((input[29]) << 1) | ((input[30]) << 22);
    output[20] = ((input[30]) >>> (21 - 11)) | ((input[31]) << 11);
  }

  private static void fastpackwithoutmask22(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 22);
    output[1] = ((input[1]) >>> (22 - 12)) | ((input[2]) << 12);
    output[2] =
        ((input[2]) >>> (22 - 2)) | ((input[3]) << 2) | ((input[4]) << 24);
    output[3] = ((input[4]) >>> (22 - 14)) | ((input[5]) << 14);
    output[4] =
        ((input[5]) >>> (22 - 4)) | ((input[6]) << 4) | ((input[7]) << 26);
    output[5] = ((input[7]) >>> (22 - 16)) | ((input[8]) << 16);
    output[6] =
        ((input[8]) >>> (22 - 6)) | ((input[9]) << 6) | ((input[10]) << 28);
    output[7] = ((input[10]) >>> (22 - 18)) | ((input[11]) << 18);
    output[8] =
        ((input[11]) >>> (22 - 8)) | ((input[12]) << 8) | ((input[13]) << 30);
    output[9] = ((input[13]) >>> (22 - 20)) | ((input[14]) << 20);
    output[10] = ((input[14]) >>> (22 - 10)) | ((input[15]) << 10);
    output[11] = input[16] | ((input[17]) << 22);
    output[12] = ((input[17]) >>> (22 - 12)) | ((input[18]) << 12);
    output[13] =
        ((input[18]) >>> (22 - 2)) | ((input[19]) << 2) | ((input[20]) << 24);
    output[14] = ((input[20]) >>> (22 - 14)) | ((input[21]) << 14);
    output[15] =
        ((input[21]) >>> (22 - 4)) | ((input[22]) << 4) | ((input[23]) << 26);
    output[16] = ((input[23]) >>> (22 - 16)) | ((input[24]) << 16);
    output[17] =
        ((input[24]) >>> (22 - 6)) | ((input[25]) << 6) | ((input[26]) << 28);
    output[18] = ((input[26]) >>> (22 - 18)) | ((input[27]) << 18);
    output[19] =
        ((input[27]) >>> (22 - 8)) | ((input[28]) << 8) | ((input[29]) << 30);
    output[20] = ((input[29]) >>> (22 - 20)) | ((input[30]) << 20);
    output[21] = ((input[30]) >>> (22 - 10)) | ((input[31]) << 10);
  }

  private static void fastpackwithoutmask23(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 23);
    output[1] = ((input[1]) >>> (23 - 14)) | ((input[2]) << 14);
    output[2] =
        ((input[2]) >>> (23 - 5)) | ((input[3]) << 5) | ((input[4]) << 28);
    output[3] = ((input[4]) >>> (23 - 19)) | ((input[5]) << 19);
    output[4] = ((input[5]) >>> (23 - 10)) | ((input[6]) << 10);
    output[5] =
        ((input[6]) >>> (23 - 1)) | ((input[7]) << 1) | ((input[8]) << 24);
    output[6] = ((input[8]) >>> (23 - 15)) | ((input[9]) << 15);
    output[7] =
        ((input[9]) >>> (23 - 6)) | ((input[10]) << 6) | ((input[11]) << 29);
    output[8] = ((input[11]) >>> (23 - 20)) | ((input[12]) << 20);
    output[9] = ((input[12]) >>> (23 - 11)) | ((input[13]) << 11);
    output[10] =
        ((input[13]) >>> (23 - 2)) | ((input[14]) << 2) | ((input[15]) << 25);
    output[11] = ((input[15]) >>> (23 - 16)) | ((input[16]) << 16);
    output[12] =
        ((input[16]) >>> (23 - 7)) | ((input[17]) << 7) | ((input[18]) << 30);
    output[13] = ((input[18]) >>> (23 - 21)) | ((input[19]) << 21);
    output[14] = ((input[19]) >>> (23 - 12)) | ((input[20]) << 12);
    output[15] =
        ((input[20]) >>> (23 - 3)) | ((input[21]) << 3) | ((input[22]) << 26);
    output[16] = ((input[22]) >>> (23 - 17)) | ((input[23]) << 17);
    output[17] =
        ((input[23]) >>> (23 - 8)) | ((input[24]) << 8) | ((input[25]) << 31);
    output[18] = ((input[25]) >>> (23 - 22)) | ((input[26]) << 22);
    output[19] = ((input[26]) >>> (23 - 13)) | ((input[27]) << 13);
    output[20] =
        ((input[27]) >>> (23 - 4)) | ((input[28]) << 4) | ((input[29]) << 27);
    output[21] = ((input[29]) >>> (23 - 18)) | ((input[30]) << 18);
    output[22] = ((input[30]) >>> (23 - 9)) | ((input[31]) << 9);
  }

  private static void fastpackwithoutmask24(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 24);
    output[1] = ((input[1]) >>> (24 - 16)) | ((input[2]) << 16);
    output[2] = ((input[2]) >>> (24 - 8)) | ((input[3]) << 8);
    output[3] = input[4] | ((input[5]) << 24);
    output[4] = ((input[5]) >>> (24 - 16)) | ((input[6]) << 16);
    output[5] = ((input[6]) >>> (24 - 8)) | ((input[7]) << 8);
    output[6] = input[8] | ((input[9]) << 24);
    output[7] = ((input[9]) >>> (24 - 16)) | ((input[10]) << 16);
    output[8] = ((input[10]) >>> (24 - 8)) | ((input[11]) << 8);
    output[9] = input[12] | ((input[13]) << 24);
    output[10] = ((input[13]) >>> (24 - 16)) | ((input[14]) << 16);
    output[11] = ((input[14]) >>> (24 - 8)) | ((input[15]) << 8);
    output[12] = input[16] | ((input[17]) << 24);
    output[13] = ((input[17]) >>> (24 - 16)) | ((input[18]) << 16);
    output[14] = ((input[18]) >>> (24 - 8)) | ((input[19]) << 8);
    output[15] = input[20] | ((input[21]) << 24);
    output[16] = ((input[21]) >>> (24 - 16)) | ((input[22]) << 16);
    output[17] = ((input[22]) >>> (24 - 8)) | ((input[23]) << 8);
    output[18] = input[24] | ((input[25]) << 24);
    output[19] = ((input[25]) >>> (24 - 16)) | ((input[26]) << 16);
    output[20] = ((input[26]) >>> (24 - 8)) | ((input[27]) << 8);
    output[21] = input[28] | ((input[29]) << 24);
    output[22] = ((input[29]) >>> (24 - 16)) | ((input[30]) << 16);
    output[23] = ((input[30]) >>> (24 - 8)) | ((input[31]) << 8);
  }

  private static void fastpackwithoutmask25(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 25);
    output[1] = ((input[1]) >>> (25 - 18)) | ((input[2]) << 18);
    output[2] = ((input[2]) >>> (25 - 11)) | ((input[3]) << 11);
    output[3] =
        ((input[3]) >>> (25 - 4)) | ((input[4]) << 4) | ((input[5]) << 29);
    output[4] = ((input[5]) >>> (25 - 22)) | ((input[6]) << 22);
    output[5] = ((input[6]) >>> (25 - 15)) | ((input[7]) << 15);
    output[6] = ((input[7]) >>> (25 - 8)) | ((input[8]) << 8);
    output[7] =
        ((input[8]) >>> (25 - 1)) | ((input[9]) << 1) | ((input[10]) << 26);
    output[8] = ((input[10]) >>> (25 - 19)) | ((input[11]) << 19);
    output[9] = ((input[11]) >>> (25 - 12)) | ((input[12]) << 12);
    output[10] =
        ((input[12]) >>> (25 - 5)) | ((input[13]) << 5) | ((input[14]) << 30);
    output[11] = ((input[14]) >>> (25 - 23)) | ((input[15]) << 23);
    output[12] = ((input[15]) >>> (25 - 16)) | ((input[16]) << 16);
    output[13] = ((input[16]) >>> (25 - 9)) | ((input[17]) << 9);
    output[14] =
        ((input[17]) >>> (25 - 2)) | ((input[18]) << 2) | ((input[19]) << 27);
    output[15] = ((input[19]) >>> (25 - 20)) | ((input[20]) << 20);
    output[16] = ((input[20]) >>> (25 - 13)) | ((input[21]) << 13);
    output[17] =
        ((input[21]) >>> (25 - 6)) | ((input[22]) << 6) | ((input[23]) << 31);
    output[18] = ((input[23]) >>> (25 - 24)) | ((input[24]) << 24);
    output[19] = ((input[24]) >>> (25 - 17)) | ((input[25]) << 17);
    output[20] = ((input[25]) >>> (25 - 10)) | ((input[26]) << 10);
    output[21] =
        ((input[26]) >>> (25 - 3)) | ((input[27]) << 3) | ((input[28]) << 28);
    output[22] = ((input[28]) >>> (25 - 21)) | ((input[29]) << 21);
    output[23] = ((input[29]) >>> (25 - 14)) | ((input[30]) << 14);
    output[24] = ((input[30]) >>> (25 - 7)) | ((input[31]) << 7);
  }

  private static void fastpackwithoutmask26(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 26);
    output[1] = ((input[1]) >>> (26 - 20)) | ((input[2]) << 20);
    output[2] = ((input[2]) >>> (26 - 14)) | ((input[3]) << 14);
    output[3] = ((input[3]) >>> (26 - 8)) | ((input[4]) << 8);
    output[4] =
        ((input[4]) >>> (26 - 2)) | ((input[5]) << 2) | ((input[6]) << 28);
    output[5] = ((input[6]) >>> (26 - 22)) | ((input[7]) << 22);
    output[6] = ((input[7]) >>> (26 - 16)) | ((input[8]) << 16);
    output[7] = ((input[8]) >>> (26 - 10)) | ((input[9]) << 10);
    output[8] =
        ((input[9]) >>> (26 - 4)) | ((input[10]) << 4) | ((input[11]) << 30);
    output[9] = ((input[11]) >>> (26 - 24)) | ((input[12]) << 24);
    output[10] = ((input[12]) >>> (26 - 18)) | ((input[13]) << 18);
    output[11] = ((input[13]) >>> (26 - 12)) | ((input[14]) << 12);
    output[12] = ((input[14]) >>> (26 - 6)) | ((input[15]) << 6);
    output[13] = input[16] | ((input[17]) << 26);
    output[14] = ((input[17]) >>> (26 - 20)) | ((input[18]) << 20);
    output[15] = ((input[18]) >>> (26 - 14)) | ((input[19]) << 14);
    output[16] = ((input[19]) >>> (26 - 8)) | ((input[20]) << 8);
    output[17] =
        ((input[20]) >>> (26 - 2)) | ((input[21]) << 2) | ((input[22]) << 28);
    output[18] = ((input[22]) >>> (26 - 22)) | ((input[23]) << 22);
    output[19] = ((input[23]) >>> (26 - 16)) | ((input[24]) << 16);
    output[20] = ((input[24]) >>> (26 - 10)) | ((input[25]) << 10);
    output[21] =
        ((input[25]) >>> (26 - 4)) | ((input[26]) << 4) | ((input[27]) << 30);
    output[22] = ((input[27]) >>> (26 - 24)) | ((input[28]) << 24);
    output[23] = ((input[28]) >>> (26 - 18)) | ((input[29]) << 18);
    output[24] = ((input[29]) >>> (26 - 12)) | ((input[30]) << 12);
    output[25] = ((input[30]) >>> (26 - 6)) | ((input[31]) << 6);
  }

  private static void fastpackwithoutmask27(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 27);
    output[1] = ((input[1]) >>> (27 - 22)) | ((input[2]) << 22);
    output[2] = ((input[2]) >>> (27 - 17)) | ((input[3]) << 17);
    output[3] = ((input[3]) >>> (27 - 12)) | ((input[4]) << 12);
    output[4] = ((input[4]) >>> (27 - 7)) | ((input[5]) << 7);
    output[5] =
        ((input[5]) >>> (27 - 2)) | ((input[6]) << 2) | ((input[7]) << 29);
    output[6] = ((input[7]) >>> (27 - 24)) | ((input[8]) << 24);
    output[7] = ((input[8]) >>> (27 - 19)) | ((input[9]) << 19);
    output[8] = ((input[9]) >>> (27 - 14)) | ((input[10]) << 14);
    output[9] = ((input[10]) >>> (27 - 9)) | ((input[11]) << 9);
    output[10] =
        ((input[11]) >>> (27 - 4)) | ((input[12]) << 4) | ((input[13]) << 31);
    output[11] = ((input[13]) >>> (27 - 26)) | ((input[14]) << 26);
    output[12] = ((input[14]) >>> (27 - 21)) | ((input[15]) << 21);
    output[13] = ((input[15]) >>> (27 - 16)) | ((input[16]) << 16);
    output[14] = ((input[16]) >>> (27 - 11)) | ((input[17]) << 11);
    output[15] = ((input[17]) >>> (27 - 6)) | ((input[18]) << 6);
    output[16] =
        ((input[18]) >>> (27 - 1)) | ((input[19]) << 1) | ((input[20]) << 28);
    output[17] = ((input[20]) >>> (27 - 23)) | ((input[21]) << 23);
    output[18] = ((input[21]) >>> (27 - 18)) | ((input[22]) << 18);
    output[19] = ((input[22]) >>> (27 - 13)) | ((input[23]) << 13);
    output[20] = ((input[23]) >>> (27 - 8)) | ((input[24]) << 8);
    output[21] =
        ((input[24]) >>> (27 - 3)) | ((input[25]) << 3) | ((input[26]) << 30);
    output[22] = ((input[26]) >>> (27 - 25)) | ((input[27]) << 25);
    output[23] = ((input[27]) >>> (27 - 20)) | ((input[28]) << 20);
    output[24] = ((input[28]) >>> (27 - 15)) | ((input[29]) << 15);
    output[25] = ((input[29]) >>> (27 - 10)) | ((input[30]) << 10);
    output[26] = ((input[30]) >>> (27 - 5)) | ((input[31]) << 5);
  }

  private static void fastpackwithoutmask28(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 28);
    output[1] = ((input[1]) >>> (28 - 24)) | ((input[2]) << 24);
    output[2] = ((input[2]) >>> (28 - 20)) | ((input[3]) << 20);
    output[3] = ((input[3]) >>> (28 - 16)) | ((input[4]) << 16);
    output[4] = ((input[4]) >>> (28 - 12)) | ((input[5]) << 12);
    output[5] = ((input[5]) >>> (28 - 8)) | ((input[6]) << 8);
    output[6] = ((input[6]) >>> (28 - 4)) | ((input[7]) << 4);
    output[7] = input[8] | ((input[9]) << 28);
    output[8] = ((input[9]) >>> (28 - 24)) | ((input[10]) << 24);
    output[9] = ((input[10]) >>> (28 - 20)) | ((input[11]) << 20);
    output[10] = ((input[11]) >>> (28 - 16)) | ((input[12]) << 16);
    output[11] = ((input[12]) >>> (28 - 12)) | ((input[13]) << 12);
    output[12] = ((input[13]) >>> (28 - 8)) | ((input[14]) << 8);
    output[13] = ((input[14]) >>> (28 - 4)) | ((input[15]) << 4);
    output[14] = input[16] | ((input[17]) << 28);
    output[15] = ((input[17]) >>> (28 - 24)) | ((input[18]) << 24);
    output[16] = ((input[18]) >>> (28 - 20)) | ((input[19]) << 20);
    output[17] = ((input[19]) >>> (28 - 16)) | ((input[20]) << 16);
    output[18] = ((input[20]) >>> (28 - 12)) | ((input[21]) << 12);
    output[19] = ((input[21]) >>> (28 - 8)) | ((input[22]) << 8);
    output[20] = ((input[22]) >>> (28 - 4)) | ((input[23]) << 4);
    output[21] = input[24] | ((input[25]) << 28);
    output[22] = ((input[25]) >>> (28 - 24)) | ((input[26]) << 24);
    output[23] = ((input[26]) >>> (28 - 20)) | ((input[27]) << 20);
    output[24] = ((input[27]) >>> (28 - 16)) | ((input[28]) << 16);
    output[25] = ((input[28]) >>> (28 - 12)) | ((input[29]) << 12);
    output[26] = ((input[29]) >>> (28 - 8)) | ((input[30]) << 8);
    output[27] = ((input[30]) >>> (28 - 4)) | ((input[31]) << 4);
  }

  private static void fastpackwithoutmask29(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 29);
    output[1] = ((input[1]) >>> (29 - 26)) | ((input[2]) << 26);
    output[2] = ((input[2]) >>> (29 - 23)) | ((input[3]) << 23);
    output[3] = ((input[3]) >>> (29 - 20)) | ((input[4]) << 20);
    output[4] = ((input[4]) >>> (29 - 17)) | ((input[5]) << 17);
    output[5] = ((input[5]) >>> (29 - 14)) | ((input[6]) << 14);
    output[6] = ((input[6]) >>> (29 - 11)) | ((input[7]) << 11);
    output[7] = ((input[7]) >>> (29 - 8)) | ((input[8]) << 8);
    output[8] = ((input[8]) >>> (29 - 5)) | ((input[9]) << 5);
    output[9] =
        ((input[9]) >>> (29 - 2)) | ((input[10]) << 2) | ((input[11]) << 31);
    output[10] = ((input[11]) >>> (29 - 28)) | ((input[12]) << 28);
    output[11] = ((input[12]) >>> (29 - 25)) | ((input[13]) << 25);
    output[12] = ((input[13]) >>> (29 - 22)) | ((input[14]) << 22);
    output[13] = ((input[14]) >>> (29 - 19)) | ((input[15]) << 19);
    output[14] = ((input[15]) >>> (29 - 16)) | ((input[16]) << 16);
    output[15] = ((input[16]) >>> (29 - 13)) | ((input[17]) << 13);
    output[16] = ((input[17]) >>> (29 - 10)) | ((input[18]) << 10);
    output[17] = ((input[18]) >>> (29 - 7)) | ((input[19]) << 7);
    output[18] = ((input[19]) >>> (29 - 4)) | ((input[20]) << 4);
    output[19] =
        ((input[20]) >>> (29 - 1)) | ((input[21]) << 1) | ((input[22]) << 30);
    output[20] = ((input[22]) >>> (29 - 27)) | ((input[23]) << 27);
    output[21] = ((input[23]) >>> (29 - 24)) | ((input[24]) << 24);
    output[22] = ((input[24]) >>> (29 - 21)) | ((input[25]) << 21);
    output[23] = ((input[25]) >>> (29 - 18)) | ((input[26]) << 18);
    output[24] = ((input[26]) >>> (29 - 15)) | ((input[27]) << 15);
    output[25] = ((input[27]) >>> (29 - 12)) | ((input[28]) << 12);
    output[26] = ((input[28]) >>> (29 - 9)) | ((input[29]) << 9);
    output[27] = ((input[29]) >>> (29 - 6)) | ((input[30]) << 6);
    output[28] = ((input[30]) >>> (29 - 3)) | ((input[31]) << 3);
  }

  private static void fastpackwithoutmask3(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 3) | ((input[2]) << 6) |
                ((input[3]) << 9) | ((input[4]) << 12) | ((input[5]) << 15) |
                ((input[6]) << 18) | ((input[7]) << 21) | ((input[8]) << 24) |
                ((input[9]) << 27) | ((input[10]) << 30);
    output[1] = ((input[10]) >>> (3 - 1)) | ((input[11]) << 1) |
                ((input[12]) << 4) | ((input[13]) << 7) | ((input[14]) << 10) |
                ((input[15]) << 13) | ((input[16]) << 16) |
                ((input[17]) << 19) | ((input[18]) << 22) |
                ((input[19]) << 25) | ((input[20]) << 28) | ((input[21]) << 31);
    output[2] = ((input[21]) >>> (3 - 2)) | ((input[22]) << 2) |
                ((input[23]) << 5) | ((input[24]) << 8) | ((input[25]) << 11) |
                ((input[26]) << 14) | ((input[27]) << 17) |
                ((input[28]) << 20) | ((input[29]) << 23) |
                ((input[30]) << 26) | ((input[31]) << 29);
  }

  private static void fastpackwithoutmask30(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 30);
    output[1] = ((input[1]) >>> (30 - 28)) | ((input[2]) << 28);
    output[2] = ((input[2]) >>> (30 - 26)) | ((input[3]) << 26);
    output[3] = ((input[3]) >>> (30 - 24)) | ((input[4]) << 24);
    output[4] = ((input[4]) >>> (30 - 22)) | ((input[5]) << 22);
    output[5] = ((input[5]) >>> (30 - 20)) | ((input[6]) << 20);
    output[6] = ((input[6]) >>> (30 - 18)) | ((input[7]) << 18);
    output[7] = ((input[7]) >>> (30 - 16)) | ((input[8]) << 16);
    output[8] = ((input[8]) >>> (30 - 14)) | ((input[9]) << 14);
    output[9] = ((input[9]) >>> (30 - 12)) | ((input[10]) << 12);
    output[10] = ((input[10]) >>> (30 - 10)) | ((input[11]) << 10);
    output[11] = ((input[11]) >>> (30 - 8)) | ((input[12]) << 8);
    output[12] = ((input[12]) >>> (30 - 6)) | ((input[13]) << 6);
    output[13] = ((input[13]) >>> (30 - 4)) | ((input[14]) << 4);
    output[14] = ((input[14]) >>> (30 - 2)) | ((input[15]) << 2);
    output[15] = input[16] | ((input[17]) << 30);
    output[16] = ((input[17]) >>> (30 - 28)) | ((input[18]) << 28);
    output[17] = ((input[18]) >>> (30 - 26)) | ((input[19]) << 26);
    output[18] = ((input[19]) >>> (30 - 24)) | ((input[20]) << 24);
    output[19] = ((input[20]) >>> (30 - 22)) | ((input[21]) << 22);
    output[20] = ((input[21]) >>> (30 - 20)) | ((input[22]) << 20);
    output[21] = ((input[22]) >>> (30 - 18)) | ((input[23]) << 18);
    output[22] = ((input[23]) >>> (30 - 16)) | ((input[24]) << 16);
    output[23] = ((input[24]) >>> (30 - 14)) | ((input[25]) << 14);
    output[24] = ((input[25]) >>> (30 - 12)) | ((input[26]) << 12);
    output[25] = ((input[26]) >>> (30 - 10)) | ((input[27]) << 10);
    output[26] = ((input[27]) >>> (30 - 8)) | ((input[28]) << 8);
    output[27] = ((input[28]) >>> (30 - 6)) | ((input[29]) << 6);
    output[28] = ((input[29]) >>> (30 - 4)) | ((input[30]) << 4);
    output[29] = ((input[30]) >>> (30 - 2)) | ((input[31]) << 2);
  }

  private static void fastpackwithoutmask31(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 31);
    output[1] = ((input[1]) >>> (31 - 30)) | ((input[2]) << 30);
    output[2] = ((input[2]) >>> (31 - 29)) | ((input[3]) << 29);
    output[3] = ((input[3]) >>> (31 - 28)) | ((input[4]) << 28);
    output[4] = ((input[4]) >>> (31 - 27)) | ((input[5]) << 27);
    output[5] = ((input[5]) >>> (31 - 26)) | ((input[6]) << 26);
    output[6] = ((input[6]) >>> (31 - 25)) | ((input[7]) << 25);
    output[7] = ((input[7]) >>> (31 - 24)) | ((input[8]) << 24);
    output[8] = ((input[8]) >>> (31 - 23)) | ((input[9]) << 23);
    output[9] = ((input[9]) >>> (31 - 22)) | ((input[10]) << 22);
    output[10] = ((input[10]) >>> (31 - 21)) | ((input[11]) << 21);
    output[11] = ((input[11]) >>> (31 - 20)) | ((input[12]) << 20);
    output[12] = ((input[12]) >>> (31 - 19)) | ((input[13]) << 19);
    output[13] = ((input[13]) >>> (31 - 18)) | ((input[14]) << 18);
    output[14] = ((input[14]) >>> (31 - 17)) | ((input[15]) << 17);
    output[15] = ((input[15]) >>> (31 - 16)) | ((input[16]) << 16);
    output[16] = ((input[16]) >>> (31 - 15)) | ((input[17]) << 15);
    output[17] = ((input[17]) >>> (31 - 14)) | ((input[18]) << 14);
    output[18] = ((input[18]) >>> (31 - 13)) | ((input[19]) << 13);
    output[19] = ((input[19]) >>> (31 - 12)) | ((input[20]) << 12);
    output[20] = ((input[20]) >>> (31 - 11)) | ((input[21]) << 11);
    output[21] = ((input[21]) >>> (31 - 10)) | ((input[22]) << 10);
    output[22] = ((input[22]) >>> (31 - 9)) | ((input[23]) << 9);
    output[23] = ((input[23]) >>> (31 - 8)) | ((input[24]) << 8);
    output[24] = ((input[24]) >>> (31 - 7)) | ((input[25]) << 7);
    output[25] = ((input[25]) >>> (31 - 6)) | ((input[26]) << 6);
    output[26] = ((input[26]) >>> (31 - 5)) | ((input[27]) << 5);
    output[27] = ((input[27]) >>> (31 - 4)) | ((input[28]) << 4);
    output[28] = ((input[28]) >>> (31 - 3)) | ((input[29]) << 3);
    output[29] = ((input[29]) >>> (31 - 2)) | ((input[30]) << 2);
    output[30] = ((input[30]) >>> (31 - 1)) | ((input[31]) << 1);
  }

  private static void fastpackwithoutmask32(int *input, int *output) {
    Unsafe.CopyBlock(output, input, 32);
  }

  private static void fastpackwithoutmask4(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 4) | ((input[2]) << 8) |
                ((input[3]) << 12) | ((input[4]) << 16) | ((input[5]) << 20) |
                ((input[6]) << 24) | ((input[7]) << 28);
    output[1] = input[8] | ((input[9]) << 4) | ((input[10]) << 8) |
                ((input[11]) << 12) | ((input[12]) << 16) |
                ((input[13]) << 20) | ((input[14]) << 24) | ((input[15]) << 28);
    output[2] = input[16] | ((input[17]) << 4) | ((input[18]) << 8) |
                ((input[19]) << 12) | ((input[20]) << 16) |
                ((input[21]) << 20) | ((input[22]) << 24) | ((input[23]) << 28);
    output[3] = input[24] | ((input[25]) << 4) | ((input[26]) << 8) |
                ((input[27]) << 12) | ((input[28]) << 16) |
                ((input[29]) << 20) | ((input[30]) << 24) | ((input[31]) << 28);
  }

  private static void fastpackwithoutmask5(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 5) | ((input[2]) << 10) |
                ((input[3]) << 15) | ((input[4]) << 20) | ((input[5]) << 25) |
                ((input[6]) << 30);
    output[1] = ((input[6]) >>> (5 - 3)) | ((input[7]) << 3) |
                ((input[8]) << 8) | ((input[9]) << 13) | ((input[10]) << 18) |
                ((input[11]) << 23) | ((input[12]) << 28);
    output[2] = ((input[12]) >>> (5 - 1)) | ((input[13]) << 1) |
                ((input[14]) << 6) | ((input[15]) << 11) | ((input[16]) << 16) |
                ((input[17]) << 21) | ((input[18]) << 26) | ((input[19]) << 31);
    output[3] = ((input[19]) >>> (5 - 4)) | ((input[20]) << 4) |
                ((input[21]) << 9) | ((input[22]) << 14) | ((input[23]) << 19) |
                ((input[24]) << 24) | ((input[25]) << 29);
    output[4] = ((input[25]) >>> (5 - 2)) | ((input[26]) << 2) |
                ((input[27]) << 7) | ((input[28]) << 12) | ((input[29]) << 17) |
                ((input[30]) << 22) | ((input[31]) << 27);
  }

  private static void fastpackwithoutmask6(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 6) | ((input[2]) << 12) |
                ((input[3]) << 18) | ((input[4]) << 24) | ((input[5]) << 30);
    output[1] = ((input[5]) >>> (6 - 4)) | ((input[6]) << 4) |
                ((input[7]) << 10) | ((input[8]) << 16) | ((input[9]) << 22) |
                ((input[10]) << 28);
    output[2] = ((input[10]) >>> (6 - 2)) | ((input[11]) << 2) |
                ((input[12]) << 8) | ((input[13]) << 14) | ((input[14]) << 20) |
                ((input[15]) << 26);
    output[3] = input[16] | ((input[17]) << 6) | ((input[18]) << 12) |
                ((input[19]) << 18) | ((input[20]) << 24) | ((input[21]) << 30);
    output[4] = ((input[21]) >>> (6 - 4)) | ((input[22]) << 4) |
                ((input[23]) << 10) | ((input[24]) << 16) |
                ((input[25]) << 22) | ((input[26]) << 28);
    output[5] = ((input[26]) >>> (6 - 2)) | ((input[27]) << 2) |
                ((input[28]) << 8) | ((input[29]) << 14) | ((input[30]) << 20) |
                ((input[31]) << 26);
  }

  private static void fastpackwithoutmask7(int *input, int *output) {
    output[0] = input[0] | ((input[1]) << 7) | ((input[2]) << 14) |
                ((input[3]) << 21) | ((input[4]) << 28);
    output[1] = ((input[4]) >>> (7 - 3)) | ((input[5]) << 3) |
                ((input[6]) << 10) | ((input[7]) << 17) | ((input[8]) << 24) |
                ((input[9]) << 31);
    output[2] = ((input[9]) >>> (7 - 6)) | ((input[10]) << 6) |
                ((input[11]) << 13) | ((input[12]) << 20) | ((input[13]) << 27);
    output[3] = ((input[13]) >>> (7 - 2)) | ((input[14]) << 2) |
                ((input[15]) << 9) | ((input[16]) << 16) | ((input[17]) << 23) |
                ((input[18]) << 30);
    output[4] = ((input[18]) >>> (7 - 5)) | ((input[19]) << 5) |
                ((input[20]) << 12) | ((input[21]) << 19) | ((input[22]) << 26);
    output[5] = ((input[22]) >>> (7 - 1)) | ((input[23]) << 1) |
                ((input[24]) << 8) | ((input[25]) << 15) | ((input[26]) << 22) |
                ((input[27]) << 29);
    output[6] = ((input[27]) >>> (7 - 4)) | ((input[28]) << 4) |
                ((input[29]) << 11) | ((input[30]) << 18) | ((input[31]) << 25);
  }

  private static void fastpackwithoutmask8(int *input, int *output) {
    output[0] =
        input[0] | ((input[1]) << 8) | ((input[2]) << 16) | ((input[3]) << 24);
    output[1] =
        input[4] | ((input[5]) << 8) | ((input[6]) << 16) | ((input[7]) << 24);
    output[2] = input[8] | ((input[9]) << 8) | ((input[10]) << 16) |
                ((input[11]) << 24);
    output[3] = input[12] | ((input[13]) << 8) | ((input[14]) << 16) |
                ((input[15]) << 24);
    output[4] = input[16] | ((input[17]) << 8) | ((input[18]) << 16) |
                ((input[19]) << 24);
    output[5] = input[20] | ((input[21]) << 8) | ((input[22]) << 16) |
                ((input[23]) << 24);
    output[6] = input[24] | ((input[25]) << 8) | ((input[26]) << 16) |
                ((input[27]) << 24);
    output[7] = input[28] | ((input[29]) << 8) | ((input[30]) << 16) |
                ((input[31]) << 24);
  }

  private static void fastpackwithoutmask9(int *input, int *output) {
    output[0] =
        input[0] | ((input[1]) << 9) | ((input[2]) << 18) | ((input[3]) << 27);
    output[1] = ((input[3]) >>> (9 - 4)) | ((input[4]) << 4) |
                ((input[5]) << 13) | ((input[6]) << 22) | ((input[7]) << 31);
    output[2] = ((input[7]) >>> (9 - 8)) | ((input[8]) << 8) |
                ((input[9]) << 17) | ((input[10]) << 26);
    output[3] = ((input[10]) >>> (9 - 3)) | ((input[11]) << 3) |
                ((input[12]) << 12) | ((input[13]) << 21) | ((input[14]) << 30);
    output[4] = ((input[14]) >>> (9 - 7)) | ((input[15]) << 7) |
                ((input[16]) << 16) | ((input[17]) << 25);
    output[5] = ((input[17]) >>> (9 - 2)) | ((input[18]) << 2) |
                ((input[19]) << 11) | ((input[20]) << 20) | ((input[21]) << 29);
    output[6] = ((input[21]) >>> (9 - 6)) | ((input[22]) << 6) |
                ((input[23]) << 15) | ((input[24]) << 24);
    output[7] = ((input[24]) >>> (9 - 1)) | ((input[25]) << 1) |
                ((input[26]) << 10) | ((input[27]) << 19) | ((input[28]) << 28);
    output[8] = ((input[28]) >>> (9 - 5)) | ((input[29]) << 5) |
                ((input[30]) << 14) | ((input[31]) << 23);
  }

  public static void Unpack32(int *input, int *output, int bit) {
    switch (bit) {
    case 0:
      fastunpack0(input, output);
      break;
    case 1:
      fastunpack1(input, output);
      break;
    case 2:
      fastunpack2(input, output);
      break;
    case 3:
      fastunpack3(input, output);
      break;
    case 4:
      fastunpack4(input, output);
      break;
    case 5:
      fastunpack5(input, output);
      break;
    case 6:
      fastunpack6(input, output);
      break;
    case 7:
      fastunpack7(input, output);
      break;
    case 8:
      fastunpack8(input, output);
      break;
    case 9:
      fastunpack9(input, output);
      break;
    case 10:
      fastunpack10(input, output);
      break;
    case 11:
      fastunpack11(input, output);
      break;
    case 12:
      fastunpack12(input, output);
      break;
    case 13:
      fastunpack13(input, output);
      break;
    case 14:
      fastunpack14(input, output);
      break;
    case 15:
      fastunpack15(input, output);
      break;
    case 16:
      fastunpack16(input, output);
      break;
    case 17:
      fastunpack17(input, output);
      break;
    case 18:
      fastunpack18(input, output);
      break;
    case 19:
      fastunpack19(input, output);
      break;
    case 20:
      fastunpack20(input, output);
      break;
    case 21:
      fastunpack21(input, output);
      break;
    case 22:
      fastunpack22(input, output);
      break;
    case 23:
      fastunpack23(input, output);
      break;
    case 24:
      fastunpack24(input, output);
      break;
    case 25:
      fastunpack25(input, output);
      break;
    case 26:
      fastunpack26(input, output);
      break;
    case 27:
      fastunpack27(input, output);
      break;
    case 28:
      fastunpack28(input, output);
      break;
    case 29:
      fastunpack29(input, output);
      break;
    case 30:
      fastunpack30(input, output);
      break;
    case 31:
      fastunpack31(input, output);
      break;
    case 32:
      fastunpack32(input, output);
      break;
    default:
      throw new NotSupportedException("Unsupported bit width.");
    }
  }

  private static void fastunpack0(int *input, int *output) {
    new Span<int>(output, 32).Clear();
  }

  private static void fastunpack1(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 1);
    output[1] = ((input[0] >>> 1) & 1);
    output[2] = ((input[0] >>> 2) & 1);
    output[3] = ((input[0] >>> 3) & 1);
    output[4] = ((input[0] >>> 4) & 1);
    output[5] = ((input[0] >>> 5) & 1);
    output[6] = ((input[0] >>> 6) & 1);
    output[7] = ((input[0] >>> 7) & 1);
    output[8] = ((input[0] >>> 8) & 1);
    output[9] = ((input[0] >>> 9) & 1);
    output[10] = ((input[0] >>> 10) & 1);
    output[11] = ((input[0] >>> 11) & 1);
    output[12] = ((input[0] >>> 12) & 1);
    output[13] = ((input[0] >>> 13) & 1);
    output[14] = ((input[0] >>> 14) & 1);
    output[15] = ((input[0] >>> 15) & 1);
    output[16] = ((input[0] >>> 16) & 1);
    output[17] = ((input[0] >>> 17) & 1);
    output[18] = ((input[0] >>> 18) & 1);
    output[19] = ((input[0] >>> 19) & 1);
    output[20] = ((input[0] >>> 20) & 1);
    output[21] = ((input[0] >>> 21) & 1);
    output[22] = ((input[0] >>> 22) & 1);
    output[23] = ((input[0] >>> 23) & 1);
    output[24] = ((input[0] >>> 24) & 1);
    output[25] = ((input[0] >>> 25) & 1);
    output[26] = ((input[0] >>> 26) & 1);
    output[27] = ((input[0] >>> 27) & 1);
    output[28] = ((input[0] >>> 28) & 1);
    output[29] = ((input[0] >>> 29) & 1);
    output[30] = ((input[0] >>> 30) & 1);
    output[31] = (input[0] >>> 31);
  }

  private static void fastunpack10(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 1023);
    output[1] = ((input[0] >>> 10) & 1023);
    output[2] = ((input[0] >>> 20) & 1023);
    output[3] = (input[0] >>> 30) | ((input[1] & 255) << (10 - 8));
    output[4] = ((input[1] >>> 8) & 1023);
    output[5] = ((input[1] >>> 18) & 1023);
    output[6] = (input[1] >>> 28) | ((input[2] & 63) << (10 - 6));
    output[7] = ((input[2] >>> 6) & 1023);
    output[8] = ((input[2] >>> 16) & 1023);
    output[9] = (input[2] >>> 26) | ((input[3] & 15) << (10 - 4));
    output[10] = ((input[3] >>> 4) & 1023);
    output[11] = ((input[3] >>> 14) & 1023);
    output[12] = (input[3] >>> 24) | ((input[4] & 3) << (10 - 2));
    output[13] = ((input[4] >>> 2) & 1023);
    output[14] = ((input[4] >>> 12) & 1023);
    output[15] = (input[4] >>> 22);
    output[16] = ((input[5] >>> 0) & 1023);
    output[17] = ((input[5] >>> 10) & 1023);
    output[18] = ((input[5] >>> 20) & 1023);
    output[19] = (input[5] >>> 30) | ((input[6] & 255) << (10 - 8));
    output[20] = ((input[6] >>> 8) & 1023);
    output[21] = ((input[6] >>> 18) & 1023);
    output[22] = (input[6] >>> 28) | ((input[7] & 63) << (10 - 6));
    output[23] = ((input[7] >>> 6) & 1023);
    output[24] = ((input[7] >>> 16) & 1023);
    output[25] = (input[7] >>> 26) | ((input[8] & 15) << (10 - 4));
    output[26] = ((input[8] >>> 4) & 1023);
    output[27] = ((input[8] >>> 14) & 1023);
    output[28] = (input[8] >>> 24) | ((input[9] & 3) << (10 - 2));
    output[29] = ((input[9] >>> 2) & 1023);
    output[30] = ((input[9] >>> 12) & 1023);
    output[31] = (input[9] >>> 22);
  }

  private static void fastunpack11(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 2047);
    output[1] = ((input[0] >>> 11) & 2047);
    output[2] = (input[0] >>> 22) | ((input[1] & 1) << (11 - 1));
    output[3] = ((input[1] >>> 1) & 2047);
    output[4] = ((input[1] >>> 12) & 2047);
    output[5] = (input[1] >>> 23) | ((input[2] & 3) << (11 - 2));
    output[6] = ((input[2] >>> 2) & 2047);
    output[7] = ((input[2] >>> 13) & 2047);
    output[8] = (input[2] >>> 24) | ((input[3] & 7) << (11 - 3));
    output[9] = ((input[3] >>> 3) & 2047);
    output[10] = ((input[3] >>> 14) & 2047);
    output[11] = (input[3] >>> 25) | ((input[4] & 15) << (11 - 4));
    output[12] = ((input[4] >>> 4) & 2047);
    output[13] = ((input[4] >>> 15) & 2047);
    output[14] = (input[4] >>> 26) | ((input[5] & 31) << (11 - 5));
    output[15] = ((input[5] >>> 5) & 2047);
    output[16] = ((input[5] >>> 16) & 2047);
    output[17] = (input[5] >>> 27) | ((input[6] & 63) << (11 - 6));
    output[18] = ((input[6] >>> 6) & 2047);
    output[19] = ((input[6] >>> 17) & 2047);
    output[20] = (input[6] >>> 28) | ((input[7] & 127) << (11 - 7));
    output[21] = ((input[7] >>> 7) & 2047);
    output[22] = ((input[7] >>> 18) & 2047);
    output[23] = (input[7] >>> 29) | ((input[8] & 255) << (11 - 8));
    output[24] = ((input[8] >>> 8) & 2047);
    output[25] = ((input[8] >>> 19) & 2047);
    output[26] = (input[8] >>> 30) | ((input[9] & 511) << (11 - 9));
    output[27] = ((input[9] >>> 9) & 2047);
    output[28] = ((input[9] >>> 20) & 2047);
    output[29] = (input[9] >>> 31) | ((input[10] & 1023) << (11 - 10));
    output[30] = ((input[10] >>> 10) & 2047);
    output[31] = (input[10] >>> 21);
  }

  private static void fastunpack12(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 4095);
    output[1] = ((input[0] >>> 12) & 4095);
    output[2] = (input[0] >>> 24) | ((input[1] & 15) << (12 - 4));
    output[3] = ((input[1] >>> 4) & 4095);
    output[4] = ((input[1] >>> 16) & 4095);
    output[5] = (input[1] >>> 28) | ((input[2] & 255) << (12 - 8));
    output[6] = ((input[2] >>> 8) & 4095);
    output[7] = (input[2] >>> 20);
    output[8] = ((input[3] >>> 0) & 4095);
    output[9] = ((input[3] >>> 12) & 4095);
    output[10] = (input[3] >>> 24) | ((input[4] & 15) << (12 - 4));
    output[11] = ((input[4] >>> 4) & 4095);
    output[12] = ((input[4] >>> 16) & 4095);
    output[13] = (input[4] >>> 28) | ((input[5] & 255) << (12 - 8));
    output[14] = ((input[5] >>> 8) & 4095);
    output[15] = (input[5] >>> 20);
    output[16] = ((input[6] >>> 0) & 4095);
    output[17] = ((input[6] >>> 12) & 4095);
    output[18] = (input[6] >>> 24) | ((input[7] & 15) << (12 - 4));
    output[19] = ((input[7] >>> 4) & 4095);
    output[20] = ((input[7] >>> 16) & 4095);
    output[21] = (input[7] >>> 28) | ((input[8] & 255) << (12 - 8));
    output[22] = ((input[8] >>> 8) & 4095);
    output[23] = (input[8] >>> 20);
    output[24] = ((input[9] >>> 0) & 4095);
    output[25] = ((input[9] >>> 12) & 4095);
    output[26] = (input[9] >>> 24) | ((input[10] & 15) << (12 - 4));
    output[27] = ((input[10] >>> 4) & 4095);
    output[28] = ((input[10] >>> 16) & 4095);
    output[29] = (input[10] >>> 28) | ((input[11] & 255) << (12 - 8));
    output[30] = ((input[11] >>> 8) & 4095);
    output[31] = (input[11] >>> 20);
  }

  private static void fastunpack13(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 8191);
    output[1] = ((input[0] >>> 13) & 8191);
    output[2] = (input[0] >>> 26) | ((input[1] & 127) << (13 - 7));
    output[3] = ((input[1] >>> 7) & 8191);
    output[4] = (input[1] >>> 20) | ((input[2] & 1) << (13 - 1));
    output[5] = ((input[2] >>> 1) & 8191);
    output[6] = ((input[2] >>> 14) & 8191);
    output[7] = (input[2] >>> 27) | ((input[3] & 255) << (13 - 8));
    output[8] = ((input[3] >>> 8) & 8191);
    output[9] = (input[3] >>> 21) | ((input[4] & 3) << (13 - 2));
    output[10] = ((input[4] >>> 2) & 8191);
    output[11] = ((input[4] >>> 15) & 8191);
    output[12] = (input[4] >>> 28) | ((input[5] & 511) << (13 - 9));
    output[13] = ((input[5] >>> 9) & 8191);
    output[14] = (input[5] >>> 22) | ((input[6] & 7) << (13 - 3));
    output[15] = ((input[6] >>> 3) & 8191);
    output[16] = ((input[6] >>> 16) & 8191);
    output[17] = (input[6] >>> 29) | ((input[7] & 1023) << (13 - 10));
    output[18] = ((input[7] >>> 10) & 8191);
    output[19] = (input[7] >>> 23) | ((input[8] & 15) << (13 - 4));
    output[20] = ((input[8] >>> 4) & 8191);
    output[21] = ((input[8] >>> 17) & 8191);
    output[22] = (input[8] >>> 30) | ((input[9] & 2047) << (13 - 11));
    output[23] = ((input[9] >>> 11) & 8191);
    output[24] = (input[9] >>> 24) | ((input[10] & 31) << (13 - 5));
    output[25] = ((input[10] >>> 5) & 8191);
    output[26] = ((input[10] >>> 18) & 8191);
    output[27] = (input[10] >>> 31) | ((input[11] & 4095) << (13 - 12));
    output[28] = ((input[11] >>> 12) & 8191);
    output[29] = (input[11] >>> 25) | ((input[12] & 63) << (13 - 6));
    output[30] = ((input[12] >>> 6) & 8191);
    output[31] = (input[12] >>> 19);
  }

  private static void fastunpack14(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 16383);
    output[1] = ((input[0] >>> 14) & 16383);
    output[2] = (input[0] >>> 28) | ((input[1] & 1023) << (14 - 10));
    output[3] = ((input[1] >>> 10) & 16383);
    output[4] = (input[1] >>> 24) | ((input[2] & 63) << (14 - 6));
    output[5] = ((input[2] >>> 6) & 16383);
    output[6] = (input[2] >>> 20) | ((input[3] & 3) << (14 - 2));
    output[7] = ((input[3] >>> 2) & 16383);
    output[8] = ((input[3] >>> 16) & 16383);
    output[9] = (input[3] >>> 30) | ((input[4] & 4095) << (14 - 12));
    output[10] = ((input[4] >>> 12) & 16383);
    output[11] = (input[4] >>> 26) | ((input[5] & 255) << (14 - 8));
    output[12] = ((input[5] >>> 8) & 16383);
    output[13] = (input[5] >>> 22) | ((input[6] & 15) << (14 - 4));
    output[14] = ((input[6] >>> 4) & 16383);
    output[15] = (input[6] >>> 18);
    output[16] = ((input[7] >>> 0) & 16383);
    output[17] = ((input[7] >>> 14) & 16383);
    output[18] = (input[7] >>> 28) | ((input[8] & 1023) << (14 - 10));
    output[19] = ((input[8] >>> 10) & 16383);
    output[20] = (input[8] >>> 24) | ((input[9] & 63) << (14 - 6));
    output[21] = ((input[9] >>> 6) & 16383);
    output[22] = (input[9] >>> 20) | ((input[10] & 3) << (14 - 2));
    output[23] = ((input[10] >>> 2) & 16383);
    output[24] = ((input[10] >>> 16) & 16383);
    output[25] = (input[10] >>> 30) | ((input[11] & 4095) << (14 - 12));
    output[26] = ((input[11] >>> 12) & 16383);
    output[27] = (input[11] >>> 26) | ((input[12] & 255) << (14 - 8));
    output[28] = ((input[12] >>> 8) & 16383);
    output[29] = (input[12] >>> 22) | ((input[13] & 15) << (14 - 4));
    output[30] = ((input[13] >>> 4) & 16383);
    output[31] = (input[13] >>> 18);
  }

  private static void fastunpack15(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 32767);
    output[1] = ((input[0] >>> 15) & 32767);
    output[2] = (input[0] >>> 30) | ((input[1] & 8191) << (15 - 13));
    output[3] = ((input[1] >>> 13) & 32767);
    output[4] = (input[1] >>> 28) | ((input[2] & 2047) << (15 - 11));
    output[5] = ((input[2] >>> 11) & 32767);
    output[6] = (input[2] >>> 26) | ((input[3] & 511) << (15 - 9));
    output[7] = ((input[3] >>> 9) & 32767);
    output[8] = (input[3] >>> 24) | ((input[4] & 127) << (15 - 7));
    output[9] = ((input[4] >>> 7) & 32767);
    output[10] = (input[4] >>> 22) | ((input[5] & 31) << (15 - 5));
    output[11] = ((input[5] >>> 5) & 32767);
    output[12] = (input[5] >>> 20) | ((input[6] & 7) << (15 - 3));
    output[13] = ((input[6] >>> 3) & 32767);
    output[14] = (input[6] >>> 18) | ((input[7] & 1) << (15 - 1));
    output[15] = ((input[7] >>> 1) & 32767);
    output[16] = ((input[7] >>> 16) & 32767);
    output[17] = (input[7] >>> 31) | ((input[8] & 16383) << (15 - 14));
    output[18] = ((input[8] >>> 14) & 32767);
    output[19] = (input[8] >>> 29) | ((input[9] & 4095) << (15 - 12));
    output[20] = ((input[9] >>> 12) & 32767);
    output[21] = (input[9] >>> 27) | ((input[10] & 1023) << (15 - 10));
    output[22] = ((input[10] >>> 10) & 32767);
    output[23] = (input[10] >>> 25) | ((input[11] & 255) << (15 - 8));
    output[24] = ((input[11] >>> 8) & 32767);
    output[25] = (input[11] >>> 23) | ((input[12] & 63) << (15 - 6));
    output[26] = ((input[12] >>> 6) & 32767);
    output[27] = (input[12] >>> 21) | ((input[13] & 15) << (15 - 4));
    output[28] = ((input[13] >>> 4) & 32767);
    output[29] = (input[13] >>> 19) | ((input[14] & 3) << (15 - 2));
    output[30] = ((input[14] >>> 2) & 32767);
    output[31] = (input[14] >>> 17);
  }

  private static void fastunpack16(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 65535);
    output[1] = (input[0] >>> 16);
    output[2] = ((input[1] >>> 0) & 65535);
    output[3] = (input[1] >>> 16);
    output[4] = ((input[2] >>> 0) & 65535);
    output[5] = (input[2] >>> 16);
    output[6] = ((input[3] >>> 0) & 65535);
    output[7] = (input[3] >>> 16);
    output[8] = ((input[4] >>> 0) & 65535);
    output[9] = (input[4] >>> 16);
    output[10] = ((input[5] >>> 0) & 65535);
    output[11] = (input[5] >>> 16);
    output[12] = ((input[6] >>> 0) & 65535);
    output[13] = (input[6] >>> 16);
    output[14] = ((input[7] >>> 0) & 65535);
    output[15] = (input[7] >>> 16);
    output[16] = ((input[8] >>> 0) & 65535);
    output[17] = (input[8] >>> 16);
    output[18] = ((input[9] >>> 0) & 65535);
    output[19] = (input[9] >>> 16);
    output[20] = ((input[10] >>> 0) & 65535);
    output[21] = (input[10] >>> 16);
    output[22] = ((input[11] >>> 0) & 65535);
    output[23] = (input[11] >>> 16);
    output[24] = ((input[12] >>> 0) & 65535);
    output[25] = (input[12] >>> 16);
    output[26] = ((input[13] >>> 0) & 65535);
    output[27] = (input[13] >>> 16);
    output[28] = ((input[14] >>> 0) & 65535);
    output[29] = (input[14] >>> 16);
    output[30] = ((input[15] >>> 0) & 65535);
    output[31] = (input[15] >>> 16);
  }

  private static void fastunpack17(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 131071);
    output[1] = (input[0] >>> 17) | ((input[1] & 3) << (17 - 2));
    output[2] = ((input[1] >>> 2) & 131071);
    output[3] = (input[1] >>> 19) | ((input[2] & 15) << (17 - 4));
    output[4] = ((input[2] >>> 4) & 131071);
    output[5] = (input[2] >>> 21) | ((input[3] & 63) << (17 - 6));
    output[6] = ((input[3] >>> 6) & 131071);
    output[7] = (input[3] >>> 23) | ((input[4] & 255) << (17 - 8));
    output[8] = ((input[4] >>> 8) & 131071);
    output[9] = (input[4] >>> 25) | ((input[5] & 1023) << (17 - 10));
    output[10] = ((input[5] >>> 10) & 131071);
    output[11] = (input[5] >>> 27) | ((input[6] & 4095) << (17 - 12));
    output[12] = ((input[6] >>> 12) & 131071);
    output[13] = (input[6] >>> 29) | ((input[7] & 16383) << (17 - 14));
    output[14] = ((input[7] >>> 14) & 131071);
    output[15] = (input[7] >>> 31) | ((input[8] & 65535) << (17 - 16));
    output[16] = (input[8] >>> 16) | ((input[9] & 1) << (17 - 1));
    output[17] = ((input[9] >>> 1) & 131071);
    output[18] = (input[9] >>> 18) | ((input[10] & 7) << (17 - 3));
    output[19] = ((input[10] >>> 3) & 131071);
    output[20] = (input[10] >>> 20) | ((input[11] & 31) << (17 - 5));
    output[21] = ((input[11] >>> 5) & 131071);
    output[22] = (input[11] >>> 22) | ((input[12] & 127) << (17 - 7));
    output[23] = ((input[12] >>> 7) & 131071);
    output[24] = (input[12] >>> 24) | ((input[13] & 511) << (17 - 9));
    output[25] = ((input[13] >>> 9) & 131071);
    output[26] = (input[13] >>> 26) | ((input[14] & 2047) << (17 - 11));
    output[27] = ((input[14] >>> 11) & 131071);
    output[28] = (input[14] >>> 28) | ((input[15] & 8191) << (17 - 13));
    output[29] = ((input[15] >>> 13) & 131071);
    output[30] = (input[15] >>> 30) | ((input[16] & 32767) << (17 - 15));
    output[31] = (input[16] >>> 15);
  }

  private static void fastunpack18(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 262143);
    output[1] = (input[0] >>> 18) | ((input[1] & 15) << (18 - 4));
    output[2] = ((input[1] >>> 4) & 262143);
    output[3] = (input[1] >>> 22) | ((input[2] & 255) << (18 - 8));
    output[4] = ((input[2] >>> 8) & 262143);
    output[5] = (input[2] >>> 26) | ((input[3] & 4095) << (18 - 12));
    output[6] = ((input[3] >>> 12) & 262143);
    output[7] = (input[3] >>> 30) | ((input[4] & 65535) << (18 - 16));
    output[8] = (input[4] >>> 16) | ((input[5] & 3) << (18 - 2));
    output[9] = ((input[5] >>> 2) & 262143);
    output[10] = (input[5] >>> 20) | ((input[6] & 63) << (18 - 6));
    output[11] = ((input[6] >>> 6) & 262143);
    output[12] = (input[6] >>> 24) | ((input[7] & 1023) << (18 - 10));
    output[13] = ((input[7] >>> 10) & 262143);
    output[14] = (input[7] >>> 28) | ((input[8] & 16383) << (18 - 14));
    output[15] = (input[8] >>> 14);
    output[16] = ((input[9] >>> 0) & 262143);
    output[17] = (input[9] >>> 18) | ((input[10] & 15) << (18 - 4));
    output[18] = ((input[10] >>> 4) & 262143);
    output[19] = (input[10] >>> 22) | ((input[11] & 255) << (18 - 8));
    output[20] = ((input[11] >>> 8) & 262143);
    output[21] = (input[11] >>> 26) | ((input[12] & 4095) << (18 - 12));
    output[22] = ((input[12] >>> 12) & 262143);
    output[23] = (input[12] >>> 30) | ((input[13] & 65535) << (18 - 16));
    output[24] = (input[13] >>> 16) | ((input[14] & 3) << (18 - 2));
    output[25] = ((input[14] >>> 2) & 262143);
    output[26] = (input[14] >>> 20) | ((input[15] & 63) << (18 - 6));
    output[27] = ((input[15] >>> 6) & 262143);
    output[28] = (input[15] >>> 24) | ((input[16] & 1023) << (18 - 10));
    output[29] = ((input[16] >>> 10) & 262143);
    output[30] = (input[16] >>> 28) | ((input[17] & 16383) << (18 - 14));
    output[31] = (input[17] >>> 14);
  }

  private static void fastunpack19(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 524287);
    output[1] = (input[0] >>> 19) | ((input[1] & 63) << (19 - 6));
    output[2] = ((input[1] >>> 6) & 524287);
    output[3] = (input[1] >>> 25) | ((input[2] & 4095) << (19 - 12));
    output[4] = ((input[2] >>> 12) & 524287);
    output[5] = (input[2] >>> 31) | ((input[3] & 262143) << (19 - 18));
    output[6] = (input[3] >>> 18) | ((input[4] & 31) << (19 - 5));
    output[7] = ((input[4] >>> 5) & 524287);
    output[8] = (input[4] >>> 24) | ((input[5] & 2047) << (19 - 11));
    output[9] = ((input[5] >>> 11) & 524287);
    output[10] = (input[5] >>> 30) | ((input[6] & 131071) << (19 - 17));
    output[11] = (input[6] >>> 17) | ((input[7] & 15) << (19 - 4));
    output[12] = ((input[7] >>> 4) & 524287);
    output[13] = (input[7] >>> 23) | ((input[8] & 1023) << (19 - 10));
    output[14] = ((input[8] >>> 10) & 524287);
    output[15] = (input[8] >>> 29) | ((input[9] & 65535) << (19 - 16));
    output[16] = (input[9] >>> 16) | ((input[10] & 7) << (19 - 3));
    output[17] = ((input[10] >>> 3) & 524287);
    output[18] = (input[10] >>> 22) | ((input[11] & 511) << (19 - 9));
    output[19] = ((input[11] >>> 9) & 524287);
    output[20] = (input[11] >>> 28) | ((input[12] & 32767) << (19 - 15));
    output[21] = (input[12] >>> 15) | ((input[13] & 3) << (19 - 2));
    output[22] = ((input[13] >>> 2) & 524287);
    output[23] = (input[13] >>> 21) | ((input[14] & 255) << (19 - 8));
    output[24] = ((input[14] >>> 8) & 524287);
    output[25] = (input[14] >>> 27) | ((input[15] & 16383) << (19 - 14));
    output[26] = (input[15] >>> 14) | ((input[16] & 1) << (19 - 1));
    output[27] = ((input[16] >>> 1) & 524287);
    output[28] = (input[16] >>> 20) | ((input[17] & 127) << (19 - 7));
    output[29] = ((input[17] >>> 7) & 524287);
    output[30] = (input[17] >>> 26) | ((input[18] & 8191) << (19 - 13));
    output[31] = (input[18] >>> 13);
  }

  private static void fastunpack2(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 3);
    output[1] = ((input[0] >>> 2) & 3);
    output[2] = ((input[0] >>> 4) & 3);
    output[3] = ((input[0] >>> 6) & 3);
    output[4] = ((input[0] >>> 8) & 3);
    output[5] = ((input[0] >>> 10) & 3);
    output[6] = ((input[0] >>> 12) & 3);
    output[7] = ((input[0] >>> 14) & 3);
    output[8] = ((input[0] >>> 16) & 3);
    output[9] = ((input[0] >>> 18) & 3);
    output[10] = ((input[0] >>> 20) & 3);
    output[11] = ((input[0] >>> 22) & 3);
    output[12] = ((input[0] >>> 24) & 3);
    output[13] = ((input[0] >>> 26) & 3);
    output[14] = ((input[0] >>> 28) & 3);
    output[15] = (input[0] >>> 30);
    output[16] = ((input[1] >>> 0) & 3);
    output[17] = ((input[1] >>> 2) & 3);
    output[18] = ((input[1] >>> 4) & 3);
    output[19] = ((input[1] >>> 6) & 3);
    output[20] = ((input[1] >>> 8) & 3);
    output[21] = ((input[1] >>> 10) & 3);
    output[22] = ((input[1] >>> 12) & 3);
    output[23] = ((input[1] >>> 14) & 3);
    output[24] = ((input[1] >>> 16) & 3);
    output[25] = ((input[1] >>> 18) & 3);
    output[26] = ((input[1] >>> 20) & 3);
    output[27] = ((input[1] >>> 22) & 3);
    output[28] = ((input[1] >>> 24) & 3);
    output[29] = ((input[1] >>> 26) & 3);
    output[30] = ((input[1] >>> 28) & 3);
    output[31] = (input[1] >>> 30);
  }

  private static void fastunpack20(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 1048575);
    output[1] = (input[0] >>> 20) | ((input[1] & 255) << (20 - 8));
    output[2] = ((input[1] >>> 8) & 1048575);
    output[3] = (input[1] >>> 28) | ((input[2] & 65535) << (20 - 16));
    output[4] = (input[2] >>> 16) | ((input[3] & 15) << (20 - 4));
    output[5] = ((input[3] >>> 4) & 1048575);
    output[6] = (input[3] >>> 24) | ((input[4] & 4095) << (20 - 12));
    output[7] = (input[4] >>> 12);
    output[8] = ((input[5] >>> 0) & 1048575);
    output[9] = (input[5] >>> 20) | ((input[6] & 255) << (20 - 8));
    output[10] = ((input[6] >>> 8) & 1048575);
    output[11] = (input[6] >>> 28) | ((input[7] & 65535) << (20 - 16));
    output[12] = (input[7] >>> 16) | ((input[8] & 15) << (20 - 4));
    output[13] = ((input[8] >>> 4) & 1048575);
    output[14] = (input[8] >>> 24) | ((input[9] & 4095) << (20 - 12));
    output[15] = (input[9] >>> 12);
    output[16] = ((input[10] >>> 0) & 1048575);
    output[17] = (input[10] >>> 20) | ((input[11] & 255) << (20 - 8));
    output[18] = ((input[11] >>> 8) & 1048575);
    output[19] = (input[11] >>> 28) | ((input[12] & 65535) << (20 - 16));
    output[20] = (input[12] >>> 16) | ((input[13] & 15) << (20 - 4));
    output[21] = ((input[13] >>> 4) & 1048575);
    output[22] = (input[13] >>> 24) | ((input[14] & 4095) << (20 - 12));
    output[23] = (input[14] >>> 12);
    output[24] = ((input[15] >>> 0) & 1048575);
    output[25] = (input[15] >>> 20) | ((input[16] & 255) << (20 - 8));
    output[26] = ((input[16] >>> 8) & 1048575);
    output[27] = (input[16] >>> 28) | ((input[17] & 65535) << (20 - 16));
    output[28] = (input[17] >>> 16) | ((input[18] & 15) << (20 - 4));
    output[29] = ((input[18] >>> 4) & 1048575);
    output[30] = (input[18] >>> 24) | ((input[19] & 4095) << (20 - 12));
    output[31] = (input[19] >>> 12);
  }

  private static void fastunpack21(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 2097151);
    output[1] = (input[0] >>> 21) | ((input[1] & 1023) << (21 - 10));
    output[2] = ((input[1] >>> 10) & 2097151);
    output[3] = (input[1] >>> 31) | ((input[2] & 1048575) << (21 - 20));
    output[4] = (input[2] >>> 20) | ((input[3] & 511) << (21 - 9));
    output[5] = ((input[3] >>> 9) & 2097151);
    output[6] = (input[3] >>> 30) | ((input[4] & 524287) << (21 - 19));
    output[7] = (input[4] >>> 19) | ((input[5] & 255) << (21 - 8));
    output[8] = ((input[5] >>> 8) & 2097151);
    output[9] = (input[5] >>> 29) | ((input[6] & 262143) << (21 - 18));
    output[10] = (input[6] >>> 18) | ((input[7] & 127) << (21 - 7));
    output[11] = ((input[7] >>> 7) & 2097151);
    output[12] = (input[7] >>> 28) | ((input[8] & 131071) << (21 - 17));
    output[13] = (input[8] >>> 17) | ((input[9] & 63) << (21 - 6));
    output[14] = ((input[9] >>> 6) & 2097151);
    output[15] = (input[9] >>> 27) | ((input[10] & 65535) << (21 - 16));
    output[16] = (input[10] >>> 16) | ((input[11] & 31) << (21 - 5));
    output[17] = ((input[11] >>> 5) & 2097151);
    output[18] = (input[11] >>> 26) | ((input[12] & 32767) << (21 - 15));
    output[19] = (input[12] >>> 15) | ((input[13] & 15) << (21 - 4));
    output[20] = ((input[13] >>> 4) & 2097151);
    output[21] = (input[13] >>> 25) | ((input[14] & 16383) << (21 - 14));
    output[22] = (input[14] >>> 14) | ((input[15] & 7) << (21 - 3));
    output[23] = ((input[15] >>> 3) & 2097151);
    output[24] = (input[15] >>> 24) | ((input[16] & 8191) << (21 - 13));
    output[25] = (input[16] >>> 13) | ((input[17] & 3) << (21 - 2));
    output[26] = ((input[17] >>> 2) & 2097151);
    output[27] = (input[17] >>> 23) | ((input[18] & 4095) << (21 - 12));
    output[28] = (input[18] >>> 12) | ((input[19] & 1) << (21 - 1));
    output[29] = ((input[19] >>> 1) & 2097151);
    output[30] = (input[19] >>> 22) | ((input[20] & 2047) << (21 - 11));
    output[31] = (input[20] >>> 11);
  }

  private static void fastunpack22(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 4194303);
    output[1] = (input[0] >>> 22) | ((input[1] & 4095) << (22 - 12));
    output[2] = (input[1] >>> 12) | ((input[2] & 3) << (22 - 2));
    output[3] = ((input[2] >>> 2) & 4194303);
    output[4] = (input[2] >>> 24) | ((input[3] & 16383) << (22 - 14));
    output[5] = (input[3] >>> 14) | ((input[4] & 15) << (22 - 4));
    output[6] = ((input[4] >>> 4) & 4194303);
    output[7] = (input[4] >>> 26) | ((input[5] & 65535) << (22 - 16));
    output[8] = (input[5] >>> 16) | ((input[6] & 63) << (22 - 6));
    output[9] = ((input[6] >>> 6) & 4194303);
    output[10] = (input[6] >>> 28) | ((input[7] & 262143) << (22 - 18));
    output[11] = (input[7] >>> 18) | ((input[8] & 255) << (22 - 8));
    output[12] = ((input[8] >>> 8) & 4194303);
    output[13] = (input[8] >>> 30) | ((input[9] & 1048575) << (22 - 20));
    output[14] = (input[9] >>> 20) | ((input[10] & 1023) << (22 - 10));
    output[15] = (input[10] >>> 10);
    output[16] = ((input[11] >>> 0) & 4194303);
    output[17] = (input[11] >>> 22) | ((input[12] & 4095) << (22 - 12));
    output[18] = (input[12] >>> 12) | ((input[13] & 3) << (22 - 2));
    output[19] = ((input[13] >>> 2) & 4194303);
    output[20] = (input[13] >>> 24) | ((input[14] & 16383) << (22 - 14));
    output[21] = (input[14] >>> 14) | ((input[15] & 15) << (22 - 4));
    output[22] = ((input[15] >>> 4) & 4194303);
    output[23] = (input[15] >>> 26) | ((input[16] & 65535) << (22 - 16));
    output[24] = (input[16] >>> 16) | ((input[17] & 63) << (22 - 6));
    output[25] = ((input[17] >>> 6) & 4194303);
    output[26] = (input[17] >>> 28) | ((input[18] & 262143) << (22 - 18));
    output[27] = (input[18] >>> 18) | ((input[19] & 255) << (22 - 8));
    output[28] = ((input[19] >>> 8) & 4194303);
    output[29] = (input[19] >>> 30) | ((input[20] & 1048575) << (22 - 20));
    output[30] = (input[20] >>> 20) | ((input[21] & 1023) << (22 - 10));
    output[31] = (input[21] >>> 10);
  }

  private static void fastunpack23(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 8388607);
    output[1] = (input[0] >>> 23) | ((input[1] & 16383) << (23 - 14));
    output[2] = (input[1] >>> 14) | ((input[2] & 31) << (23 - 5));
    output[3] = ((input[2] >>> 5) & 8388607);
    output[4] = (input[2] >>> 28) | ((input[3] & 524287) << (23 - 19));
    output[5] = (input[3] >>> 19) | ((input[4] & 1023) << (23 - 10));
    output[6] = (input[4] >>> 10) | ((input[5] & 1) << (23 - 1));
    output[7] = ((input[5] >>> 1) & 8388607);
    output[8] = (input[5] >>> 24) | ((input[6] & 32767) << (23 - 15));
    output[9] = (input[6] >>> 15) | ((input[7] & 63) << (23 - 6));
    output[10] = ((input[7] >>> 6) & 8388607);
    output[11] = (input[7] >>> 29) | ((input[8] & 1048575) << (23 - 20));
    output[12] = (input[8] >>> 20) | ((input[9] & 2047) << (23 - 11));
    output[13] = (input[9] >>> 11) | ((input[10] & 3) << (23 - 2));
    output[14] = ((input[10] >>> 2) & 8388607);
    output[15] = (input[10] >>> 25) | ((input[11] & 65535) << (23 - 16));
    output[16] = (input[11] >>> 16) | ((input[12] & 127) << (23 - 7));
    output[17] = ((input[12] >>> 7) & 8388607);
    output[18] = (input[12] >>> 30) | ((input[13] & 2097151) << (23 - 21));
    output[19] = (input[13] >>> 21) | ((input[14] & 4095) << (23 - 12));
    output[20] = (input[14] >>> 12) | ((input[15] & 7) << (23 - 3));
    output[21] = ((input[15] >>> 3) & 8388607);
    output[22] = (input[15] >>> 26) | ((input[16] & 131071) << (23 - 17));
    output[23] = (input[16] >>> 17) | ((input[17] & 255) << (23 - 8));
    output[24] = ((input[17] >>> 8) & 8388607);
    output[25] = (input[17] >>> 31) | ((input[18] & 4194303) << (23 - 22));
    output[26] = (input[18] >>> 22) | ((input[19] & 8191) << (23 - 13));
    output[27] = (input[19] >>> 13) | ((input[20] & 15) << (23 - 4));
    output[28] = ((input[20] >>> 4) & 8388607);
    output[29] = (input[20] >>> 27) | ((input[21] & 262143) << (23 - 18));
    output[30] = (input[21] >>> 18) | ((input[22] & 511) << (23 - 9));
    output[31] = (input[22] >>> 9);
  }

  private static void fastunpack24(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 16777215);
    output[1] = (input[0] >>> 24) | ((input[1] & 65535) << (24 - 16));
    output[2] = (input[1] >>> 16) | ((input[2] & 255) << (24 - 8));
    output[3] = (input[2] >>> 8);
    output[4] = ((input[3] >>> 0) & 16777215);
    output[5] = (input[3] >>> 24) | ((input[4] & 65535) << (24 - 16));
    output[6] = (input[4] >>> 16) | ((input[5] & 255) << (24 - 8));
    output[7] = (input[5] >>> 8);
    output[8] = ((input[6] >>> 0) & 16777215);
    output[9] = (input[6] >>> 24) | ((input[7] & 65535) << (24 - 16));
    output[10] = (input[7] >>> 16) | ((input[8] & 255) << (24 - 8));
    output[11] = (input[8] >>> 8);
    output[12] = ((input[9] >>> 0) & 16777215);
    output[13] = (input[9] >>> 24) | ((input[10] & 65535) << (24 - 16));
    output[14] = (input[10] >>> 16) | ((input[11] & 255) << (24 - 8));
    output[15] = (input[11] >>> 8);
    output[16] = ((input[12] >>> 0) & 16777215);
    output[17] = (input[12] >>> 24) | ((input[13] & 65535) << (24 - 16));
    output[18] = (input[13] >>> 16) | ((input[14] & 255) << (24 - 8));
    output[19] = (input[14] >>> 8);
    output[20] = ((input[15] >>> 0) & 16777215);
    output[21] = (input[15] >>> 24) | ((input[16] & 65535) << (24 - 16));
    output[22] = (input[16] >>> 16) | ((input[17] & 255) << (24 - 8));
    output[23] = (input[17] >>> 8);
    output[24] = ((input[18] >>> 0) & 16777215);
    output[25] = (input[18] >>> 24) | ((input[19] & 65535) << (24 - 16));
    output[26] = (input[19] >>> 16) | ((input[20] & 255) << (24 - 8));
    output[27] = (input[20] >>> 8);
    output[28] = ((input[21] >>> 0) & 16777215);
    output[29] = (input[21] >>> 24) | ((input[22] & 65535) << (24 - 16));
    output[30] = (input[22] >>> 16) | ((input[23] & 255) << (24 - 8));
    output[31] = (input[23] >>> 8);
  }

  private static void fastunpack25(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 33554431);
    output[1] = (input[0] >>> 25) | ((input[1] & 262143) << (25 - 18));
    output[2] = (input[1] >>> 18) | ((input[2] & 2047) << (25 - 11));
    output[3] = (input[2] >>> 11) | ((input[3] & 15) << (25 - 4));
    output[4] = ((input[3] >>> 4) & 33554431);
    output[5] = (input[3] >>> 29) | ((input[4] & 4194303) << (25 - 22));
    output[6] = (input[4] >>> 22) | ((input[5] & 32767) << (25 - 15));
    output[7] = (input[5] >>> 15) | ((input[6] & 255) << (25 - 8));
    output[8] = (input[6] >>> 8) | ((input[7] & 1) << (25 - 1));
    output[9] = ((input[7] >>> 1) & 33554431);
    output[10] = (input[7] >>> 26) | ((input[8] & 524287) << (25 - 19));
    output[11] = (input[8] >>> 19) | ((input[9] & 4095) << (25 - 12));
    output[12] = (input[9] >>> 12) | ((input[10] & 31) << (25 - 5));
    output[13] = ((input[10] >>> 5) & 33554431);
    output[14] = (input[10] >>> 30) | ((input[11] & 8388607) << (25 - 23));
    output[15] = (input[11] >>> 23) | ((input[12] & 65535) << (25 - 16));
    output[16] = (input[12] >>> 16) | ((input[13] & 511) << (25 - 9));
    output[17] = (input[13] >>> 9) | ((input[14] & 3) << (25 - 2));
    output[18] = ((input[14] >>> 2) & 33554431);
    output[19] = (input[14] >>> 27) | ((input[15] & 1048575) << (25 - 20));
    output[20] = (input[15] >>> 20) | ((input[16] & 8191) << (25 - 13));
    output[21] = (input[16] >>> 13) | ((input[17] & 63) << (25 - 6));
    output[22] = ((input[17] >>> 6) & 33554431);
    output[23] = (input[17] >>> 31) | ((input[18] & 16777215) << (25 - 24));
    output[24] = (input[18] >>> 24) | ((input[19] & 131071) << (25 - 17));
    output[25] = (input[19] >>> 17) | ((input[20] & 1023) << (25 - 10));
    output[26] = (input[20] >>> 10) | ((input[21] & 7) << (25 - 3));
    output[27] = ((input[21] >>> 3) & 33554431);
    output[28] = (input[21] >>> 28) | ((input[22] & 2097151) << (25 - 21));
    output[29] = (input[22] >>> 21) | ((input[23] & 16383) << (25 - 14));
    output[30] = (input[23] >>> 14) | ((input[24] & 127) << (25 - 7));
    output[31] = (input[24] >>> 7);
  }

  private static void fastunpack26(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 67108863);
    output[1] = (input[0] >>> 26) | ((input[1] & 1048575) << (26 - 20));
    output[2] = (input[1] >>> 20) | ((input[2] & 16383) << (26 - 14));
    output[3] = (input[2] >>> 14) | ((input[3] & 255) << (26 - 8));
    output[4] = (input[3] >>> 8) | ((input[4] & 3) << (26 - 2));
    output[5] = ((input[4] >>> 2) & 67108863);
    output[6] = (input[4] >>> 28) | ((input[5] & 4194303) << (26 - 22));
    output[7] = (input[5] >>> 22) | ((input[6] & 65535) << (26 - 16));
    output[8] = (input[6] >>> 16) | ((input[7] & 1023) << (26 - 10));
    output[9] = (input[7] >>> 10) | ((input[8] & 15) << (26 - 4));
    output[10] = ((input[8] >>> 4) & 67108863);
    output[11] = (input[8] >>> 30) | ((input[9] & 16777215) << (26 - 24));
    output[12] = (input[9] >>> 24) | ((input[10] & 262143) << (26 - 18));
    output[13] = (input[10] >>> 18) | ((input[11] & 4095) << (26 - 12));
    output[14] = (input[11] >>> 12) | ((input[12] & 63) << (26 - 6));
    output[15] = (input[12] >>> 6);
    output[16] = ((input[13] >>> 0) & 67108863);
    output[17] = (input[13] >>> 26) | ((input[14] & 1048575) << (26 - 20));
    output[18] = (input[14] >>> 20) | ((input[15] & 16383) << (26 - 14));
    output[19] = (input[15] >>> 14) | ((input[16] & 255) << (26 - 8));
    output[20] = (input[16] >>> 8) | ((input[17] & 3) << (26 - 2));
    output[21] = ((input[17] >>> 2) & 67108863);
    output[22] = (input[17] >>> 28) | ((input[18] & 4194303) << (26 - 22));
    output[23] = (input[18] >>> 22) | ((input[19] & 65535) << (26 - 16));
    output[24] = (input[19] >>> 16) | ((input[20] & 1023) << (26 - 10));
    output[25] = (input[20] >>> 10) | ((input[21] & 15) << (26 - 4));
    output[26] = ((input[21] >>> 4) & 67108863);
    output[27] = (input[21] >>> 30) | ((input[22] & 16777215) << (26 - 24));
    output[28] = (input[22] >>> 24) | ((input[23] & 262143) << (26 - 18));
    output[29] = (input[23] >>> 18) | ((input[24] & 4095) << (26 - 12));
    output[30] = (input[24] >>> 12) | ((input[25] & 63) << (26 - 6));
    output[31] = (input[25] >>> 6);
  }

  private static void fastunpack27(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 134217727);
    output[1] = (input[0] >>> 27) | ((input[1] & 4194303) << (27 - 22));
    output[2] = (input[1] >>> 22) | ((input[2] & 131071) << (27 - 17));
    output[3] = (input[2] >>> 17) | ((input[3] & 4095) << (27 - 12));
    output[4] = (input[3] >>> 12) | ((input[4] & 127) << (27 - 7));
    output[5] = (input[4] >>> 7) | ((input[5] & 3) << (27 - 2));
    output[6] = ((input[5] >>> 2) & 134217727);
    output[7] = (input[5] >>> 29) | ((input[6] & 16777215) << (27 - 24));
    output[8] = (input[6] >>> 24) | ((input[7] & 524287) << (27 - 19));
    output[9] = (input[7] >>> 19) | ((input[8] & 16383) << (27 - 14));
    output[10] = (input[8] >>> 14) | ((input[9] & 511) << (27 - 9));
    output[11] = (input[9] >>> 9) | ((input[10] & 15) << (27 - 4));
    output[12] = ((input[10] >>> 4) & 134217727);
    output[13] = (input[10] >>> 31) | ((input[11] & 67108863) << (27 - 26));
    output[14] = (input[11] >>> 26) | ((input[12] & 2097151) << (27 - 21));
    output[15] = (input[12] >>> 21) | ((input[13] & 65535) << (27 - 16));
    output[16] = (input[13] >>> 16) | ((input[14] & 2047) << (27 - 11));
    output[17] = (input[14] >>> 11) | ((input[15] & 63) << (27 - 6));
    output[18] = (input[15] >>> 6) | ((input[16] & 1) << (27 - 1));
    output[19] = ((input[16] >>> 1) & 134217727);
    output[20] = (input[16] >>> 28) | ((input[17] & 8388607) << (27 - 23));
    output[21] = (input[17] >>> 23) | ((input[18] & 262143) << (27 - 18));
    output[22] = (input[18] >>> 18) | ((input[19] & 8191) << (27 - 13));
    output[23] = (input[19] >>> 13) | ((input[20] & 255) << (27 - 8));
    output[24] = (input[20] >>> 8) | ((input[21] & 7) << (27 - 3));
    output[25] = ((input[21] >>> 3) & 134217727);
    output[26] = (input[21] >>> 30) | ((input[22] & 33554431) << (27 - 25));
    output[27] = (input[22] >>> 25) | ((input[23] & 1048575) << (27 - 20));
    output[28] = (input[23] >>> 20) | ((input[24] & 32767) << (27 - 15));
    output[29] = (input[24] >>> 15) | ((input[25] & 1023) << (27 - 10));
    output[30] = (input[25] >>> 10) | ((input[26] & 31) << (27 - 5));
    output[31] = (input[26] >>> 5);
  }

  private static void fastunpack28(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 268435455);
    output[1] = (input[0] >>> 28) | ((input[1] & 16777215) << (28 - 24));
    output[2] = (input[1] >>> 24) | ((input[2] & 1048575) << (28 - 20));
    output[3] = (input[2] >>> 20) | ((input[3] & 65535) << (28 - 16));
    output[4] = (input[3] >>> 16) | ((input[4] & 4095) << (28 - 12));
    output[5] = (input[4] >>> 12) | ((input[5] & 255) << (28 - 8));
    output[6] = (input[5] >>> 8) | ((input[6] & 15) << (28 - 4));
    output[7] = (input[6] >>> 4);
    output[8] = ((input[7] >>> 0) & 268435455);
    output[9] = (input[7] >>> 28) | ((input[8] & 16777215) << (28 - 24));
    output[10] = (input[8] >>> 24) | ((input[9] & 1048575) << (28 - 20));
    output[11] = (input[9] >>> 20) | ((input[10] & 65535) << (28 - 16));
    output[12] = (input[10] >>> 16) | ((input[11] & 4095) << (28 - 12));
    output[13] = (input[11] >>> 12) | ((input[12] & 255) << (28 - 8));
    output[14] = (input[12] >>> 8) | ((input[13] & 15) << (28 - 4));
    output[15] = (input[13] >>> 4);
    output[16] = ((input[14] >>> 0) & 268435455);
    output[17] = (input[14] >>> 28) | ((input[15] & 16777215) << (28 - 24));
    output[18] = (input[15] >>> 24) | ((input[16] & 1048575) << (28 - 20));
    output[19] = (input[16] >>> 20) | ((input[17] & 65535) << (28 - 16));
    output[20] = (input[17] >>> 16) | ((input[18] & 4095) << (28 - 12));
    output[21] = (input[18] >>> 12) | ((input[19] & 255) << (28 - 8));
    output[22] = (input[19] >>> 8) | ((input[20] & 15) << (28 - 4));
    output[23] = (input[20] >>> 4);
    output[24] = ((input[21] >>> 0) & 268435455);
    output[25] = (input[21] >>> 28) | ((input[22] & 16777215) << (28 - 24));
    output[26] = (input[22] >>> 24) | ((input[23] & 1048575) << (28 - 20));
    output[27] = (input[23] >>> 20) | ((input[24] & 65535) << (28 - 16));
    output[28] = (input[24] >>> 16) | ((input[25] & 4095) << (28 - 12));
    output[29] = (input[25] >>> 12) | ((input[26] & 255) << (28 - 8));
    output[30] = (input[26] >>> 8) | ((input[27] & 15) << (28 - 4));
    output[31] = (input[27] >>> 4);
  }

  private static void fastunpack29(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 536870911);
    output[1] = (input[0] >>> 29) | ((input[1] & 67108863) << (29 - 26));
    output[2] = (input[1] >>> 26) | ((input[2] & 8388607) << (29 - 23));
    output[3] = (input[2] >>> 23) | ((input[3] & 1048575) << (29 - 20));
    output[4] = (input[3] >>> 20) | ((input[4] & 131071) << (29 - 17));
    output[5] = (input[4] >>> 17) | ((input[5] & 16383) << (29 - 14));
    output[6] = (input[5] >>> 14) | ((input[6] & 2047) << (29 - 11));
    output[7] = (input[6] >>> 11) | ((input[7] & 255) << (29 - 8));
    output[8] = (input[7] >>> 8) | ((input[8] & 31) << (29 - 5));
    output[9] = (input[8] >>> 5) | ((input[9] & 3) << (29 - 2));
    output[10] = ((input[9] >>> 2) & 536870911);
    output[11] = (input[9] >>> 31) | ((input[10] & 268435455) << (29 - 28));
    output[12] = (input[10] >>> 28) | ((input[11] & 33554431) << (29 - 25));
    output[13] = (input[11] >>> 25) | ((input[12] & 4194303) << (29 - 22));
    output[14] = (input[12] >>> 22) | ((input[13] & 524287) << (29 - 19));
    output[15] = (input[13] >>> 19) | ((input[14] & 65535) << (29 - 16));
    output[16] = (input[14] >>> 16) | ((input[15] & 8191) << (29 - 13));
    output[17] = (input[15] >>> 13) | ((input[16] & 1023) << (29 - 10));
    output[18] = (input[16] >>> 10) | ((input[17] & 127) << (29 - 7));
    output[19] = (input[17] >>> 7) | ((input[18] & 15) << (29 - 4));
    output[20] = (input[18] >>> 4) | ((input[19] & 1) << (29 - 1));
    output[21] = ((input[19] >>> 1) & 536870911);
    output[22] = (input[19] >>> 30) | ((input[20] & 134217727) << (29 - 27));
    output[23] = (input[20] >>> 27) | ((input[21] & 16777215) << (29 - 24));
    output[24] = (input[21] >>> 24) | ((input[22] & 2097151) << (29 - 21));
    output[25] = (input[22] >>> 21) | ((input[23] & 262143) << (29 - 18));
    output[26] = (input[23] >>> 18) | ((input[24] & 32767) << (29 - 15));
    output[27] = (input[24] >>> 15) | ((input[25] & 4095) << (29 - 12));
    output[28] = (input[25] >>> 12) | ((input[26] & 511) << (29 - 9));
    output[29] = (input[26] >>> 9) | ((input[27] & 63) << (29 - 6));
    output[30] = (input[27] >>> 6) | ((input[28] & 7) << (29 - 3));
    output[31] = (input[28] >>> 3);
  }

  private static void fastunpack3(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 7);
    output[1] = ((input[0] >>> 3) & 7);
    output[2] = ((input[0] >>> 6) & 7);
    output[3] = ((input[0] >>> 9) & 7);
    output[4] = ((input[0] >>> 12) & 7);
    output[5] = ((input[0] >>> 15) & 7);
    output[6] = ((input[0] >>> 18) & 7);
    output[7] = ((input[0] >>> 21) & 7);
    output[8] = ((input[0] >>> 24) & 7);
    output[9] = ((input[0] >>> 27) & 7);
    output[10] = (input[0] >>> 30) | ((input[1] & 1) << (3 - 1));
    output[11] = ((input[1] >>> 1) & 7);
    output[12] = ((input[1] >>> 4) & 7);
    output[13] = ((input[1] >>> 7) & 7);
    output[14] = ((input[1] >>> 10) & 7);
    output[15] = ((input[1] >>> 13) & 7);
    output[16] = ((input[1] >>> 16) & 7);
    output[17] = ((input[1] >>> 19) & 7);
    output[18] = ((input[1] >>> 22) & 7);
    output[19] = ((input[1] >>> 25) & 7);
    output[20] = ((input[1] >>> 28) & 7);
    output[21] = (input[1] >>> 31) | ((input[2] & 3) << (3 - 2));
    output[22] = ((input[2] >>> 2) & 7);
    output[23] = ((input[2] >>> 5) & 7);
    output[24] = ((input[2] >>> 8) & 7);
    output[25] = ((input[2] >>> 11) & 7);
    output[26] = ((input[2] >>> 14) & 7);
    output[27] = ((input[2] >>> 17) & 7);
    output[28] = ((input[2] >>> 20) & 7);
    output[29] = ((input[2] >>> 23) & 7);
    output[30] = ((input[2] >>> 26) & 7);
    output[31] = (input[2] >>> 29);
  }

  private static void fastunpack30(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 1073741823);
    output[1] = (input[0] >>> 30) | ((input[1] & 268435455) << (30 - 28));
    output[2] = (input[1] >>> 28) | ((input[2] & 67108863) << (30 - 26));
    output[3] = (input[2] >>> 26) | ((input[3] & 16777215) << (30 - 24));
    output[4] = (input[3] >>> 24) | ((input[4] & 4194303) << (30 - 22));
    output[5] = (input[4] >>> 22) | ((input[5] & 1048575) << (30 - 20));
    output[6] = (input[5] >>> 20) | ((input[6] & 262143) << (30 - 18));
    output[7] = (input[6] >>> 18) | ((input[7] & 65535) << (30 - 16));
    output[8] = (input[7] >>> 16) | ((input[8] & 16383) << (30 - 14));
    output[9] = (input[8] >>> 14) | ((input[9] & 4095) << (30 - 12));
    output[10] = (input[9] >>> 12) | ((input[10] & 1023) << (30 - 10));
    output[11] = (input[10] >>> 10) | ((input[11] & 255) << (30 - 8));
    output[12] = (input[11] >>> 8) | ((input[12] & 63) << (30 - 6));
    output[13] = (input[12] >>> 6) | ((input[13] & 15) << (30 - 4));
    output[14] = (input[13] >>> 4) | ((input[14] & 3) << (30 - 2));
    output[15] = (input[14] >>> 2);
    output[16] = ((input[15] >>> 0) & 1073741823);
    output[17] = (input[15] >>> 30) | ((input[16] & 268435455) << (30 - 28));
    output[18] = (input[16] >>> 28) | ((input[17] & 67108863) << (30 - 26));
    output[19] = (input[17] >>> 26) | ((input[18] & 16777215) << (30 - 24));
    output[20] = (input[18] >>> 24) | ((input[19] & 4194303) << (30 - 22));
    output[21] = (input[19] >>> 22) | ((input[20] & 1048575) << (30 - 20));
    output[22] = (input[20] >>> 20) | ((input[21] & 262143) << (30 - 18));
    output[23] = (input[21] >>> 18) | ((input[22] & 65535) << (30 - 16));
    output[24] = (input[22] >>> 16) | ((input[23] & 16383) << (30 - 14));
    output[25] = (input[23] >>> 14) | ((input[24] & 4095) << (30 - 12));
    output[26] = (input[24] >>> 12) | ((input[25] & 1023) << (30 - 10));
    output[27] = (input[25] >>> 10) | ((input[26] & 255) << (30 - 8));
    output[28] = (input[26] >>> 8) | ((input[27] & 63) << (30 - 6));
    output[29] = (input[27] >>> 6) | ((input[28] & 15) << (30 - 4));
    output[30] = (input[28] >>> 4) | ((input[29] & 3) << (30 - 2));
    output[31] = (input[29] >>> 2);
  }

  private static void fastunpack31(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 2147483647);
    output[1] = (input[0] >>> 31) | ((input[1] & 1073741823) << (31 - 30));
    output[2] = (input[1] >>> 30) | ((input[2] & 536870911) << (31 - 29));
    output[3] = (input[2] >>> 29) | ((input[3] & 268435455) << (31 - 28));
    output[4] = (input[3] >>> 28) | ((input[4] & 134217727) << (31 - 27));
    output[5] = (input[4] >>> 27) | ((input[5] & 67108863) << (31 - 26));
    output[6] = (input[5] >>> 26) | ((input[6] & 33554431) << (31 - 25));
    output[7] = (input[6] >>> 25) | ((input[7] & 16777215) << (31 - 24));
    output[8] = (input[7] >>> 24) | ((input[8] & 8388607) << (31 - 23));
    output[9] = (input[8] >>> 23) | ((input[9] & 4194303) << (31 - 22));
    output[10] = (input[9] >>> 22) | ((input[10] & 2097151) << (31 - 21));
    output[11] = (input[10] >>> 21) | ((input[11] & 1048575) << (31 - 20));
    output[12] = (input[11] >>> 20) | ((input[12] & 524287) << (31 - 19));
    output[13] = (input[12] >>> 19) | ((input[13] & 262143) << (31 - 18));
    output[14] = (input[13] >>> 18) | ((input[14] & 131071) << (31 - 17));
    output[15] = (input[14] >>> 17) | ((input[15] & 65535) << (31 - 16));
    output[16] = (input[15] >>> 16) | ((input[16] & 32767) << (31 - 15));
    output[17] = (input[16] >>> 15) | ((input[17] & 16383) << (31 - 14));
    output[18] = (input[17] >>> 14) | ((input[18] & 8191) << (31 - 13));
    output[19] = (input[18] >>> 13) | ((input[19] & 4095) << (31 - 12));
    output[20] = (input[19] >>> 12) | ((input[20] & 2047) << (31 - 11));
    output[21] = (input[20] >>> 11) | ((input[21] & 1023) << (31 - 10));
    output[22] = (input[21] >>> 10) | ((input[22] & 511) << (31 - 9));
    output[23] = (input[22] >>> 9) | ((input[23] & 255) << (31 - 8));
    output[24] = (input[23] >>> 8) | ((input[24] & 127) << (31 - 7));
    output[25] = (input[24] >>> 7) | ((input[25] & 63) << (31 - 6));
    output[26] = (input[25] >>> 6) | ((input[26] & 31) << (31 - 5));
    output[27] = (input[26] >>> 5) | ((input[27] & 15) << (31 - 4));
    output[28] = (input[27] >>> 4) | ((input[28] & 7) << (31 - 3));
    output[29] = (input[28] >>> 3) | ((input[29] & 3) << (31 - 2));
    output[30] = (input[29] >>> 2) | ((input[30] & 1) << (31 - 1));
    output[31] = (input[30] >>> 1);
  }

  private static void fastunpack32(int *input, int *output) {
    Unsafe.CopyBlock(output, input, 32);
  }

  private static void fastunpack4(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 15);
    output[1] = ((input[0] >>> 4) & 15);
    output[2] = ((input[0] >>> 8) & 15);
    output[3] = ((input[0] >>> 12) & 15);
    output[4] = ((input[0] >>> 16) & 15);
    output[5] = ((input[0] >>> 20) & 15);
    output[6] = ((input[0] >>> 24) & 15);
    output[7] = (input[0] >>> 28);
    output[8] = ((input[1] >>> 0) & 15);
    output[9] = ((input[1] >>> 4) & 15);
    output[10] = ((input[1] >>> 8) & 15);
    output[11] = ((input[1] >>> 12) & 15);
    output[12] = ((input[1] >>> 16) & 15);
    output[13] = ((input[1] >>> 20) & 15);
    output[14] = ((input[1] >>> 24) & 15);
    output[15] = (input[1] >>> 28);
    output[16] = ((input[2] >>> 0) & 15);
    output[17] = ((input[2] >>> 4) & 15);
    output[18] = ((input[2] >>> 8) & 15);
    output[19] = ((input[2] >>> 12) & 15);
    output[20] = ((input[2] >>> 16) & 15);
    output[21] = ((input[2] >>> 20) & 15);
    output[22] = ((input[2] >>> 24) & 15);
    output[23] = (input[2] >>> 28);
    output[24] = ((input[3] >>> 0) & 15);
    output[25] = ((input[3] >>> 4) & 15);
    output[26] = ((input[3] >>> 8) & 15);
    output[27] = ((input[3] >>> 12) & 15);
    output[28] = ((input[3] >>> 16) & 15);
    output[29] = ((input[3] >>> 20) & 15);
    output[30] = ((input[3] >>> 24) & 15);
    output[31] = (input[3] >>> 28);
  }

  private static void fastunpack5(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 31);
    output[1] = ((input[0] >>> 5) & 31);
    output[2] = ((input[0] >>> 10) & 31);
    output[3] = ((input[0] >>> 15) & 31);
    output[4] = ((input[0] >>> 20) & 31);
    output[5] = ((input[0] >>> 25) & 31);
    output[6] = (input[0] >>> 30) | ((input[1] & 7) << (5 - 3));
    output[7] = ((input[1] >>> 3) & 31);
    output[8] = ((input[1] >>> 8) & 31);
    output[9] = ((input[1] >>> 13) & 31);
    output[10] = ((input[1] >>> 18) & 31);
    output[11] = ((input[1] >>> 23) & 31);
    output[12] = (input[1] >>> 28) | ((input[2] & 1) << (5 - 1));
    output[13] = ((input[2] >>> 1) & 31);
    output[14] = ((input[2] >>> 6) & 31);
    output[15] = ((input[2] >>> 11) & 31);
    output[16] = ((input[2] >>> 16) & 31);
    output[17] = ((input[2] >>> 21) & 31);
    output[18] = ((input[2] >>> 26) & 31);
    output[19] = (input[2] >>> 31) | ((input[3] & 15) << (5 - 4));
    output[20] = ((input[3] >>> 4) & 31);
    output[21] = ((input[3] >>> 9) & 31);
    output[22] = ((input[3] >>> 14) & 31);
    output[23] = ((input[3] >>> 19) & 31);
    output[24] = ((input[3] >>> 24) & 31);
    output[25] = (input[3] >>> 29) | ((input[4] & 3) << (5 - 2));
    output[26] = ((input[4] >>> 2) & 31);
    output[27] = ((input[4] >>> 7) & 31);
    output[28] = ((input[4] >>> 12) & 31);
    output[29] = ((input[4] >>> 17) & 31);
    output[30] = ((input[4] >>> 22) & 31);
    output[31] = (input[4] >>> 27);
  }

  private static void fastunpack6(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 63);
    output[1] = ((input[0] >>> 6) & 63);
    output[2] = ((input[0] >>> 12) & 63);
    output[3] = ((input[0] >>> 18) & 63);
    output[4] = ((input[0] >>> 24) & 63);
    output[5] = (input[0] >>> 30) | ((input[1] & 15) << (6 - 4));
    output[6] = ((input[1] >>> 4) & 63);
    output[7] = ((input[1] >>> 10) & 63);
    output[8] = ((input[1] >>> 16) & 63);
    output[9] = ((input[1] >>> 22) & 63);
    output[10] = (input[1] >>> 28) | ((input[2] & 3) << (6 - 2));
    output[11] = ((input[2] >>> 2) & 63);
    output[12] = ((input[2] >>> 8) & 63);
    output[13] = ((input[2] >>> 14) & 63);
    output[14] = ((input[2] >>> 20) & 63);
    output[15] = (input[2] >>> 26);
    output[16] = ((input[3] >>> 0) & 63);
    output[17] = ((input[3] >>> 6) & 63);
    output[18] = ((input[3] >>> 12) & 63);
    output[19] = ((input[3] >>> 18) & 63);
    output[20] = ((input[3] >>> 24) & 63);
    output[21] = (input[3] >>> 30) | ((input[4] & 15) << (6 - 4));
    output[22] = ((input[4] >>> 4) & 63);
    output[23] = ((input[4] >>> 10) & 63);
    output[24] = ((input[4] >>> 16) & 63);
    output[25] = ((input[4] >>> 22) & 63);
    output[26] = (input[4] >>> 28) | ((input[5] & 3) << (6 - 2));
    output[27] = ((input[5] >>> 2) & 63);
    output[28] = ((input[5] >>> 8) & 63);
    output[29] = ((input[5] >>> 14) & 63);
    output[30] = ((input[5] >>> 20) & 63);
    output[31] = (input[5] >>> 26);
  }

  private static void fastunpack7(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 127);
    output[1] = ((input[0] >>> 7) & 127);
    output[2] = ((input[0] >>> 14) & 127);
    output[3] = ((input[0] >>> 21) & 127);
    output[4] = (input[0] >>> 28) | ((input[1] & 7) << (7 - 3));
    output[5] = ((input[1] >>> 3) & 127);
    output[6] = ((input[1] >>> 10) & 127);
    output[7] = ((input[1] >>> 17) & 127);
    output[8] = ((input[1] >>> 24) & 127);
    output[9] = (input[1] >>> 31) | ((input[2] & 63) << (7 - 6));
    output[10] = ((input[2] >>> 6) & 127);
    output[11] = ((input[2] >>> 13) & 127);
    output[12] = ((input[2] >>> 20) & 127);
    output[13] = (input[2] >>> 27) | ((input[3] & 3) << (7 - 2));
    output[14] = ((input[3] >>> 2) & 127);
    output[15] = ((input[3] >>> 9) & 127);
    output[16] = ((input[3] >>> 16) & 127);
    output[17] = ((input[3] >>> 23) & 127);
    output[18] = (input[3] >>> 30) | ((input[4] & 31) << (7 - 5));
    output[19] = ((input[4] >>> 5) & 127);
    output[20] = ((input[4] >>> 12) & 127);
    output[21] = ((input[4] >>> 19) & 127);
    output[22] = (input[4] >>> 26) | ((input[5] & 1) << (7 - 1));
    output[23] = ((input[5] >>> 1) & 127);
    output[24] = ((input[5] >>> 8) & 127);
    output[25] = ((input[5] >>> 15) & 127);
    output[26] = ((input[5] >>> 22) & 127);
    output[27] = (input[5] >>> 29) | ((input[6] & 15) << (7 - 4));
    output[28] = ((input[6] >>> 4) & 127);
    output[29] = ((input[6] >>> 11) & 127);
    output[30] = ((input[6] >>> 18) & 127);
    output[31] = (input[6] >>> 25);
  }

  private static void fastunpack8(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 255);
    output[1] = ((input[0] >>> 8) & 255);
    output[2] = ((input[0] >>> 16) & 255);
    output[3] = (input[0] >>> 24);
    output[4] = ((input[1] >>> 0) & 255);
    output[5] = ((input[1] >>> 8) & 255);
    output[6] = ((input[1] >>> 16) & 255);
    output[7] = (input[1] >>> 24);
    output[8] = ((input[2] >>> 0) & 255);
    output[9] = ((input[2] >>> 8) & 255);
    output[10] = ((input[2] >>> 16) & 255);
    output[11] = (input[2] >>> 24);
    output[12] = ((input[3] >>> 0) & 255);
    output[13] = ((input[3] >>> 8) & 255);
    output[14] = ((input[3] >>> 16) & 255);
    output[15] = (input[3] >>> 24);
    output[16] = ((input[4] >>> 0) & 255);
    output[17] = ((input[4] >>> 8) & 255);
    output[18] = ((input[4] >>> 16) & 255);
    output[19] = (input[4] >>> 24);
    output[20] = ((input[5] >>> 0) & 255);
    output[21] = ((input[5] >>> 8) & 255);
    output[22] = ((input[5] >>> 16) & 255);
    output[23] = (input[5] >>> 24);
    output[24] = ((input[6] >>> 0) & 255);
    output[25] = ((input[6] >>> 8) & 255);
    output[26] = ((input[6] >>> 16) & 255);
    output[27] = (input[6] >>> 24);
    output[28] = ((input[7] >>> 0) & 255);
    output[29] = ((input[7] >>> 8) & 255);
    output[30] = ((input[7] >>> 16) & 255);
    output[31] = (input[7] >>> 24);
  }

  private static void fastunpack9(int *input, int *output) {
    output[0] = ((input[0] >>> 0) & 511);
    output[1] = ((input[0] >>> 9) & 511);
    output[2] = ((input[0] >>> 18) & 511);
    output[3] = (input[0] >>> 27) | ((input[1] & 15) << (9 - 4));
    output[4] = ((input[1] >>> 4) & 511);
    output[5] = ((input[1] >>> 13) & 511);
    output[6] = ((input[1] >>> 22) & 511);
    output[7] = (input[1] >>> 31) | ((input[2] & 255) << (9 - 8));
    output[8] = ((input[2] >>> 8) & 511);
    output[9] = ((input[2] >>> 17) & 511);
    output[10] = (input[2] >>> 26) | ((input[3] & 7) << (9 - 3));
    output[11] = ((input[3] >>> 3) & 511);
    output[12] = ((input[3] >>> 12) & 511);
    output[13] = ((input[3] >>> 21) & 511);
    output[14] = (input[3] >>> 30) | ((input[4] & 127) << (9 - 7));
    output[15] = ((input[4] >>> 7) & 511);
    output[16] = ((input[4] >>> 16) & 511);
    output[17] = (input[4] >>> 25) | ((input[5] & 3) << (9 - 2));
    output[18] = ((input[5] >>> 2) & 511);
    output[19] = ((input[5] >>> 11) & 511);
    output[20] = ((input[5] >>> 20) & 511);
    output[21] = (input[5] >>> 29) | ((input[6] & 63) << (9 - 6));
    output[22] = ((input[6] >>> 6) & 511);
    output[23] = ((input[6] >>> 15) & 511);
    output[24] = (input[6] >>> 24) | ((input[7] & 1) << (9 - 1));
    output[25] = ((input[7] >>> 1) & 511);
    output[26] = ((input[7] >>> 10) & 511);
    output[27] = ((input[7] >>> 19) & 511);
    output[28] = (input[7] >>> 28) | ((input[8] & 31) << (9 - 5));
    output[29] = ((input[8] >>> 5) & 511);
    output[30] = ((input[8] >>> 14) & 511);
    output[31] = (input[8] >>> 23);
  }
  
}
