using System;
using System.Text;
using Corax.Pipeline;
using Corax.Pipeline.Parsing;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Pipeline
{
    public class ParsingTests(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, this is an ASCII - only test string!")]
        [InlineData("こんにちは, this string contains non-ASCII characters!")]
        [InlineData("The quick brown fox jumps over the lazy dog. Zażółć gęślą jaźń, 你好, Привет!")]
        public void AsciiDetect(string stringToCheck)
        {
            bool IsAsciiRef(byte[] input)
            {
                foreach (byte b in input)
                {
                    if (b >= 0b10000000)
                        return false;
                }
                return true;
            }

            var bytes = Encoding.UTF8.GetBytes(stringToCheck);

            var isAsciiRef = IsAsciiRef(bytes);
            Assert.Equal(isAsciiRef, ScalarParsers.IsAscii(bytes));
            if (AdvInstructionSet.X86.IsSupportedSse)
                Assert.Equal(isAsciiRef, VectorParsers.IsAsciiSse(bytes));

            Assert.Equal(isAsciiRef, StandardParsers.IsAscii(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!", 13)]
        [InlineData("Привет, мир!", 12)]
        [InlineData("こんにちは、世界！", 9)]
        [InlineData("🙂🙃😀😃", 4)]
        [InlineData("🐘", 1)]
        [InlineData("안녕하세요, 세계!", 10)]
        [InlineData("The quick brown 🦊 jumps over the lazy 🐶. What a wonderful day! ", 63)]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ", 115)]
        public void CountCodePoints(string input, int expectedLength)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            Assert.Equal(expectedLength, ScalarParsers.CountCodePointsFromUtf8(bytes));
            if (AdvInstructionSet.X86.IsSupportedSse)
                Assert.Equal(expectedLength, VectorParsers.CountCodePointsFromUtf8(bytes));

            Assert.Equal(expectedLength, StandardParsers.CountCodePointsFromUtf8(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!")]
        [InlineData("Привет, мир!")]
        [InlineData("こんにちは、世界！")]
        [InlineData("🙂🙃😀😃")]
        [InlineData("🐘")]
        [InlineData("안녕하세요, 세계!")]
        [InlineData("The quick brown \U0001f98a jumps over the lazy 🐶. What a wonderful day! ")]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ")]
        public void Utf16LengthFromUtf8(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            Assert.Equal(input.Length, ScalarParsers.Utf16LengthFromUtf8(bytes));
            if (AdvInstructionSet.X86.IsSupportedSse)
                Assert.Equal(input.Length, VectorParsers.Utf16LengthFromUtf8(bytes));

            Assert.Equal(input.Length, StandardParsers.Utf16LengthFromUtf8(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!")]
        [InlineData("Hello\tWorld\n")]
        [InlineData("\u000B\f\r\u001C\u001D\u001E\u001F")]
        [InlineData("\t\n\u000B\f\r\u001C\u001D\u001E\u001F\t")]
        [InlineData("Whitespace\tat\nthe\u000Bend\f")]
        public void CountWhitespaces(string input)
        {
            static int CountWhitespacesRef(ReadOnlySpan<byte> buffer)
            {
                int whitespaceCount = 0;

                foreach (byte b in buffer)
                {
                    switch (b)
                    {
                        case (byte)'\t':
                        case (byte)'\n':
                        case (byte)'\f':
                        case (byte)'\r':
                        case (byte)'\u000B':
                        case (byte)'\u001C':
                        case (byte)'\u001D':
                        case (byte)'\u001E':
                        case (byte)'\u001F':
                        case (byte)' ':
                            whitespaceCount++;
                            break;
                    }
                }

                return whitespaceCount;
            }

            var bytes = Encoding.UTF8.GetBytes(input);
            var referenceLength = CountWhitespacesRef(bytes);

            Assert.Equal(referenceLength, ScalarParsers.CountWhitespacesAscii(bytes));
            if (AdvInstructionSet.X86.IsSupportedSse)
                Assert.Equal(referenceLength, VectorParsers.CountWhitespacesAscii(bytes));

            Assert.Equal(referenceLength, StandardParsers.CountWhitespacesAscii(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!", 2)]
        [InlineData("Hello,  World!", 2)]
        [InlineData("Привет, мир!", 2)]
        [InlineData("こんにちは、世界！", 1)]
        [InlineData("🙂🙃😀😃", 1)]
        [InlineData("안녕하세요, 세계!", 2)]
        [InlineData("The quick brown \U0001f98a jumps over the lazy 🐶. What a wonderful day! ", 13)]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ", 22)]
        [InlineData("One day, a terrible dragon  attacked the kingdom,  and the queen had to use her magical powers to save her people. ", 21)]
        [InlineData("\u000B\f\r\u001C\u001D\u001E\u001F", 0)]
        [InlineData("\u0009\u000A\u000B\u000C\u000D\u001C\u001D\u001E\u001F ", 0)]
        [InlineData("Whitespace\tat\nthe\u000Bend\f", 4)]
        [InlineData("Hello,                                                          World!", 2)]
        [InlineData("    Hello,             World!", 2)]
        [InlineData("", 0)]
        [InlineData("                        ", 0)]
        public void WhitespaceTokenizer(string input, int expectedTokens)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            var tokenArray = new Token[128];

            var tokens = tokenArray.AsSpan();
            ScalarTokenizers.TokenizeWhitespaceAsciiScalar(bytes, ref tokens);
            Assert.Equal(expectedTokens, tokens.Length);
            if (AdvInstructionSet.X86.IsSupportedSse)
            {
                tokens = tokenArray.AsSpan();
                VectorTokenizers.TokenizeWhitespaceAsciiSse(bytes, ref tokens);
                Assert.Equal(expectedTokens, tokens.Length);
            }

            tokens = tokenArray.AsSpan();
            ScalarTokenizers.TokenizeWhitespace(input, ref tokens);
            Assert.Equal(expectedTokens, tokens.Length);
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!")]
        [InlineData("Hello,  World!")]
        [InlineData("Привет, мир!")]
        [InlineData("こんにちは、世界！")]
        [InlineData("🙂🙃😀😃")]
        [InlineData("안녕하세요, 세계!")]
        [InlineData("The quick brown \U0001f98a jumps over the lazy 🐶. What a wonderful day! ")]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ")]
        [InlineData("One day, a terrible dragon  attacked the kingdom,  and the queen had to use her magical powers to save her people. ")]
        [InlineData("\u000B\f\r\u001C\u001D\u001E\u001F")]
        [InlineData("\u0009\u000A\u000B\u000C\u000D\u001C\u001D\u001E\u001F ")]
        [InlineData("Whitespace\tat\nthe\u000Bend\f")]
        [InlineData("Hello,                                                          World!")]
        [InlineData("H   A     Z   [   @    AZ[")]
        [InlineData("    Hello,             World!")]
        [InlineData("")]
        [InlineData("                        ")]
        [InlineData("Book2 ČĐŽŠĆ")]
        [InlineData("Book2 čĐŽŠĆ")]
        [InlineData("ÀÁÂÃÄÅ ÇÈÉÊË ÌÍÎÏ àáâãäå çèéêë ìíîï")]
        [InlineData("ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩ αβγδεζηθικλμνξοπρστυφχψω")]
        [InlineData("АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ абвгдеёжзийклмнопрстуфхцчшщъыьэюя")]
        [InlineData("I İ Ş Ğ Ü Ö Ç i i ş ğ ü ö ç")]
        [InlineData("Hello Привет こんにちは hello привет こんにちは")]
        [InlineData("ÄÖÜ äöü ß äöü äöü ß")]
        [InlineData("Iıİi")]
        [InlineData("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞĀĂĄĆĈĊČĎĐĒĔĖĘĚĜĞĠĢĤĦĨĪĬĮİĲĴĶĹĻĽĿŁŃŅŇŊŌŎŐŒŔŖŘŚŜŞŠŢŤŦŨŪŬŮŰŲŴŶŸŹŻŽƁƂƄƆƇƊƋƎƏƐƑƓƔƖƗƘƜƝƠƢƤƧƩƬƮƯƱƲƳƵƷƸƼǄǅǇǈǊǋǍǏǑǓǕǗǙǛǞǠǢǤǦǨǪǬǮǱǲǴǶǷǸǺǼǾȀȂȄȆȈȊȌȎȐȒȔȖȘȚȜȞȠȢȤȦȨȪȬȮȰȲȺȻȽȾɁɃɄɅɆɈɊɌɎͰͲͶͿΆΈΉΊΌΎΏΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΪΫϏϘϚϜϞϠϢϤϦϨϪϬϮϴϷϹϺϽϾϿЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯѠѢѤѦѨѪѬѮѰѲѴѶѸѺѼѾҀҊҌҎҐҒҔҖҘҚҜҞҠҢҤҦҨҪҬҮҰҲҴҶҸҺҼҾӀӁӃӅӇӉӋӍӐӒӔӖӘӚӜӞӠӢӤӦӨӪӬӮӰӲӴӶӸӺӼӾԀԂԄԆԈԊԌԎԐԒԔԖԘԚԜԞԠԢԤԦԨԪԬԮԱԲԳԴԵԶԷԸԹԺԻԼԽԾԿՀՁՂՃՄՅՆՇՈՉՊՋՌՍՎՏՐՑՒՓՔՕՖႠႡႢႣႤႥႦႧႨႩႪႫႬႭႮႯႰႱႲႳႴႵႶႷႸႹႺႻႼႽႾႿჀჁჂჃჄჅჇჍᎠᎡᎢᎣᎤᎥᎦᎧᎨᎩᎪᎫᎬᎭᎮᎯᎰᎱᎲᎳᎴᎵᎶᎷᎸᎹᎺᎻᎼᎽᎾᎿᏀᏁᏂᏃᏄᏅᏆᏇᏈᏉᏊᏋᏌᏍᏎᏏᏐᏑᏒᏓᏔᏕᏖᏗᏘᏙᏚᏛᏜᏝᏞᏟᏠᏡᏢᏣᏤᏥᏦᏧᏨᏩᏪᏫᏬᏭᏮᏯᏰᏱᏲᏳᏴᏵ\u1c90\u1c91\u1c92\u1c93\u1c94\u1c95\u1c96\u1c97\u1c98\u1c99\u1c9a\u1c9b\u1c9c\u1c9d\u1c9e\u1c9f\u1ca0\u1ca1\u1ca2\u1ca3\u1ca4\u1ca5\u1ca6\u1ca7\u1ca8\u1ca9\u1caa\u1cab\u1cac\u1cad\u1cae\u1caf\u1cb0\u1cb1\u1cb2\u1cb3\u1cb4\u1cb5\u1cb6\u1cb7\u1cb8\u1cb9\u1cba\u1cbd\u1cbe\u1cbfḀḂḄḆḈḊḌḎḐḒḔḖḘḚḜḞḠḢḤḦḨḪḬḮḰḲḴḶḸḺḼḾṀṂṄṆṈṊṌṎṐṒṔṖṘṚṜṞṠṢṤṦṨṪṬṮṰṲṴṶṸṺṼṾẀẂẄẆẈẊẌẎẐẒẔẞẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶỸỺỼỾἈἉἊἋἌἍἎἏἘἙἚἛἜἝἨἩἪἫἬἭἮἯἸἹἺἻἼἽἾἿὈὉὊὋὌὍὙὛὝὟὨὩὪὫὬὭὮὯᾈᾉᾊᾋᾌᾍᾎᾏᾘᾙᾚᾛᾜᾝᾞᾟᾨᾩᾪᾫᾬᾭᾮᾯᾸᾹᾺΆᾼῈΈῊΉῌῘῙῚΊῨῩῪΎῬῸΌῺΏῼⰀⰁⰂⰃⰄⰅⰆⰇⰈⰉⰊⰋⰌⰍⰎⰏⰐⰑⰒⰓⰔⰕⰖⰗⰘⰙⰚⰛⰜⰝⰞⰟⰠⰡⰢⰣⰤⰥⰦⰧⰨⰩⰪⰫⰬⰭⰮⱠⱢⱣⱤⱧⱩⱫⱭⱮⱯⱰⱲⱵⱾⱿⲀⲂⲄⲆⲈⲊⲌⲎⲐⲒⲔⲖⲘⲚⲜⲞⲠⲢⲤⲦⲨⲪⲬⲮⲰⲲⲴⲶⲸⲺⲼⲾⳀⳂⳄⳆⳈⳊⳌⳎⳐⳒⳔⳖⳘⳚⳜⳞⳠⳢⳫⳭⳲⴀⴁⴂⴃⴄⴅⴆⴇⴈⴉⴊⴋⴌⴍⴎⴏⴐⴑⴒⴓⴔⴕⴖⴗⴘⴙⴚⴛⴜⴝⴞⴟⴠⴡⴢⴣⴤⴥⴧⴭꙀꙂꙄꙆꙈꙊꙌꙎꙐꙒꙔꙖꙘꙚꙜꙞꙠꙢꙤꙦꙨꙪꙬꚀꚂꚄꚆꚈꚊꚌꚎꚐꚒꚔꚖꚘꚚꜢꜤꜦꜨꜪꜬꜮꜲꜴꜶꜸꜺꜼꜾꝀꝂꝄꝆꝈꝊꝌꝎꝐꝒꝔꝖꝘꝚꝜꝞꝠꝢꝤꝦꝨꝪꝬꝮꝹꝻꝽꝾꞀꞂꞄꞆꞋꞍꞐꞒꞖꞘꞚꞜꞞꞠꞢꞤꞦꞨꞪꞫꞬꞭ\ua7aeꞰꞱꞲꞳꞴꞶ\ua7b8\ua7ba\ua7bc\ua7be\ua7c2\ua7c4\ua7c5\ua7c6\ua7c7\ua7c9\ua7f5ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ\ud801\udc00\ud801\udc01\ud801\udc02\ud801\udc03\ud801\udc04\ud801\udc05\ud801\udc06\ud801\udc07\ud801\udc08\ud801\udc09\ud801\udc0a\ud801\udc0b\ud801\udc0c\ud801\udc0d\ud801\udc0e\ud801\udc0f\ud801\udc10\ud801\udc11\ud801\udc12\ud801\udc13\ud801\udc14\ud801\udc15\ud801\udc16\ud801\udc17\ud801\udc18\ud801\udc19\ud801\udc1a\ud801\udc1b\ud801\udc1c\ud801\udc1d\ud801\udc1e\ud801\udc1f\ud801\udc20\ud801\udc21\ud801\udc22\ud801\udc23\ud801\udc24\ud801\udc25\ud801\udc26\ud801\udc27\ud801\udcb0\ud801\udcb1\ud801\udcb2\ud801\udcb3\ud801\udcb4\ud801\udcb5\ud801\udcb6\ud801\udcb7\ud801\udcb8\ud801\udcb9\ud801\udcba\ud801\udcbb\ud801\udcbc\ud801\udcbd\ud801\udcbe\ud801\udcbf\ud801\udcc0\ud801\udcc1\ud801\udcc2\ud801\udcc3\ud801\udcc4\ud801\udcc5\ud801\udcc6\ud801\udcc7\ud801\udcc8\ud801\udcc9\ud801\udcca\ud801\udccb\ud801\udccc\ud801\udccd\ud801\udcce\ud801\udccf\ud801\udcd0\ud801\udcd1\ud801\udcd2\ud801\udcd3\ud803\udc80\ud803\udc81\ud803\udc82\ud803\udc83\ud803\udc84\ud803\udc85\ud803\udc86\ud803\udc87\ud803\udc88\ud803\udc89\ud803\udc8a\ud803\udc8b\ud803\udc8c\ud803\udc8d\ud803\udc8e\ud803\udc8f\ud803\udc90\ud803\udc91\ud803\udc92\ud803\udc93\ud803\udc94\ud803\udc95\ud803\udc96\ud803\udc97\ud803\udc98\ud803\udc99\ud803\udc9a\ud803\udc9b\ud803\udc9c\ud803\udc9d\ud803\udc9e\ud803\udc9f\ud803\udca0\ud803\udca1\ud803\udca2\ud803\udca3\ud803\udca4\ud803\udca5\ud803\udca6\ud803\udca7\ud803\udca8\ud803\udca9\ud803\udcaa\ud803\udcab\ud803\udcac\ud803\udcad\ud803\udcae\ud803\udcaf\ud803\udcb0\ud803\udcb1\ud803\udcb2\ud806\udca0\ud806\udca1\ud806\udca2\ud806\udca3\ud806\udca4\ud806\udca5\ud806\udca6\ud806\udca7\ud806\udca8\ud806\udca9\ud806\udcaa\ud806\udcab\ud806\udcac\ud806\udcad\ud806\udcae\ud806\udcaf\ud806\udcb0\ud806\udcb1\ud806\udcb2\ud806\udcb3\ud806\udcb4\ud806\udcb5\ud806\udcb6\ud806\udcb7\ud806\udcb8\ud806\udcb9\ud806\udcba\ud806\udcbb\ud806\udcbc\ud806\udcbd\ud806\udcbe\ud806\udcbf\ud81b\ude40\ud81b\ude41\ud81b\ude42\ud81b\ude43\ud81b\ude44\ud81b\ude45\ud81b\ude46\ud81b\ude47\ud81b\ude48\ud81b\ude49\ud81b\ude4a\ud81b\ude4b\ud81b\ude4c\ud81b\ude4d\ud81b\ude4e\ud81b\ude4f\ud81b\ude50\ud81b\ude51\ud81b\ude52\ud81b\ude53\ud81b\ude54\ud81b\ude55\ud81b\ude56\ud81b\ude57\ud81b\ude58\ud81b\ude59\ud81b\ude5a\ud81b\ude5b\ud81b\ude5c\ud81b\ude5d\ud81b\ude5e\ud81b\ude5f\ud83a\udd00\ud83a\udd01\ud83a\udd02\ud83a\udd03\ud83a\udd04\ud83a\udd05\ud83a\udd06\ud83a\udd07\ud83a\udd08\ud83a\udd09\ud83a\udd0a\ud83a\udd0b\ud83a\udd0c\ud83a\udd0d\ud83a\udd0e\ud83a\udd0f\ud83a\udd10\ud83a\udd11\ud83a\udd12\ud83a\udd13\ud83a\udd14\ud83a\udd15\ud83a\udd16\ud83a\udd17\ud83a\udd18\ud83a\udd19\ud83a\udd1a\ud83a\udd1b\ud83a\udd1c\ud83a\udd1d\ud83a\udd1e\ud83a\udd1f\ud83a\udd20\ud83a\udd21")]
        public void LowercaseTransformer(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var lowercaseBytes = Encoding.UTF8.GetBytes(input.ToLowerInvariant()).AsSpan();
            var lowercaseChars = input.ToLowerInvariant().AsSpan();

            var dest = new byte[bytes.Length];
            var tokenArray = new Token[128];

            var tokens = tokenArray.AsSpan();
            var destBytes = dest.AsSpan();

            ScalarTransformers.ToLowercase(bytes, tokens, ref destBytes, ref tokens);
            Assert.True(lowercaseBytes.SequenceEqual(destBytes));

            destBytes.Fill(0);

            LowerCaseTransformer transformer;
            transformer.Transform(bytes, tokens, ref destBytes, ref tokens);
            Assert.True(lowercaseBytes.SequenceEqual(destBytes));

            destBytes.Fill(0);
            
            tokens = tokenArray.AsSpan();
            destBytes = dest.AsSpan();
            Span<char> destChars = new char[bytes.Length];
            ScalarTransformers.ToLowercase(input.AsSpan(), tokens, ref destChars, ref tokens);
            Assert.True(lowercaseChars.SequenceEqual(destChars));
        }
    }
}
